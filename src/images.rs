//! Image processing pipeline for product images.
//!
//! Downloads, removes background, and caches results in SQLite.

use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use image::{DynamicImage, GenericImageView, ImageFormat};
use reqwest::Client;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Result of processing a single image URL.
#[derive(Debug, Clone)]
pub struct ProcessedImage {
    pub url: String,
    pub id: String,
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub is_transparent: bool,
    pub from_cache: bool,
}

/// Interface for background removal implementations.
pub trait BackgroundRemover: Send + Sync {
    fn remove_background(&self, image: DynamicImage) -> Result<DynamicImage>;
}

/// No-op remover used as a fallback.
pub struct NoopRemover;

impl BackgroundRemover for NoopRemover {
    fn remove_background(&self, image: DynamicImage) -> Result<DynamicImage> {
        Ok(image)
    }
}

/// Background remover powered by the rmbg crate (enabled with the `rmbg` feature).
#[cfg(feature = "rmbg")]
pub struct RmbgRemover {
    session: rmbg::Rmbg,
}

#[cfg(feature = "rmbg")]
impl RmbgRemover {
    pub fn new(model_bytes: &[u8]) -> Result<Self> {
        let session = rmbg::Rmbg::new(model_bytes)
            .context("Failed to initialize rmbg session")?;
        Ok(Self { session })
    }
}

#[cfg(feature = "rmbg")]
impl BackgroundRemover for RmbgRemover {
    fn remove_background(&self, image: DynamicImage) -> Result<DynamicImage> {
        self.session
            .remove_background(&image)
            .context("rmbg failed to remove background")
    }
}

/// Processor for downloading and caching product images.
#[derive(Clone)]
pub struct ImageProcessor {
    pool: SqlitePool,
    client: Client,
    semaphore: Arc<Semaphore>,
    remover: Arc<dyn BackgroundRemover>,
    rembg_endpoint: Option<String>,
}

const DEFAULT_REMBG_ENDPOINT: &str = "http://127.0.0.1:5000";
const REMBG_REMOVE_PATH: &str = "/api/remove";
const THUMBNAIL_SIZE: u32 = 112;

struct Thumbnail {
    bytes: Vec<u8>,
    content_type: String,
    width: u32,
    height: u32,
}

impl ImageProcessor {
    /// Create a new processor with a custom background remover.
    pub async fn new(pool: SqlitePool, remover: Arc<dyn BackgroundRemover>) -> Result<Self> {
        // Enable WAL for better concurrency during large syncs.
        sqlx::query("PRAGMA journal_mode=WAL;")
            .execute(&pool)
            .await?;

        let max_concurrency = std::cmp::max(1, num_cpus::get() / 2);
        let max_concurrency = std::cmp::min(8, max_concurrency);
        Ok(Self {
            pool,
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            remover,
            rembg_endpoint: None,
        })
    }

    /// Upgrade semaphore concurrency for HTTP rembg mode (network-bound, not CPU-bound).
    fn upgrade_concurrency_for_http(&mut self) {
        self.semaphore = Arc::new(Semaphore::new(16));
    }

    /// Create a processor that uses the rembg HTTP server for background removal.
    pub async fn new_rembg_http(
        pool: SqlitePool,
        endpoint: impl Into<String>,
    ) -> Result<Self> {
        let mut processor = Self::new(pool, Arc::new(NoopRemover)).await?;
        processor.rembg_endpoint = Some(normalize_rembg_endpoint(endpoint.into()));
        processor.upgrade_concurrency_for_http();
        Ok(processor)
    }

    /// Create a processor that uses the default rembg HTTP endpoint.
    pub async fn new_rembg_http_default(pool: SqlitePool) -> Result<Self> {
        Self::new_rembg_http(pool, DEFAULT_REMBG_ENDPOINT).await
    }

    /// Process a batch of URLs with bounded concurrency.
    pub async fn process_batch(&self, urls: Vec<String>) -> Result<Vec<ProcessedImage>> {
        if urls.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::new();
        let mut tasks = FuturesUnordered::new();

        for url in urls {
            if !seen.insert(url.clone()) {
                continue;
            }

            let processor = self.clone();
            let permit = self.semaphore.clone().acquire_owned().await?; 
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                processor.process_one(&url).await
            }));
        }

        let mut results = Vec::new();
        while let Some(joined) = tasks.next().await {
            let result = joined.context("Image processing task panicked")??;
            results.push(result);
        }

        Ok(results)
    }

    async fn process_one(&self, url: &str) -> Result<ProcessedImage> {
        let id = hash_url(url);

        if let Some(cached) = self.fetch_cached(&id).await? {
            return Ok(cached);
        }

        let response = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to download image: {url}"))?
            .error_for_status()?;

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let original_bytes = response.bytes().await?.to_vec();

        let processed = if let Some(endpoint) = &self.rembg_endpoint {
            match self.remove_background_http(endpoint, &original_bytes).await {
                Ok(png_bytes) => ProcessedImage {
                    url: url.to_string(),
                    id: id.clone(),
                    bytes: png_bytes,
                    content_type: Some("image/png".to_string()),
                    is_transparent: true,
                    from_cache: false,
                },
                Err(err) => {
                    tracing::warn!(
                        url = url,
                        error = %err,
                        "rembg HTTP failed; falling back to original image"
                    );
                    fallback_result(url, &id, &original_bytes, content_type.clone())
                }
            }
        } else {
            match decode_image(&original_bytes) {
                Ok(image) => match self.remover.remove_background(image) {
                    Ok(removed) => match encode_png(&removed) {
                        Ok(png_bytes) => ProcessedImage {
                            url: url.to_string(),
                            id: id.clone(),
                            bytes: png_bytes,
                            content_type: Some("image/png".to_string()),
                            is_transparent: true,
                            from_cache: false,
                        },
                        Err(_) => fallback_result(url, &id, &original_bytes, content_type.clone()),
                    },
                    Err(_) => fallback_result(url, &id, &original_bytes, content_type.clone()),
                },
                Err(_) => fallback_result(url, &id, &original_bytes, content_type.clone()),
            }
        };

        self.store_cached(&processed).await?;

        if let Some(thumbnail) = generate_thumbnail(&processed.bytes)
            .or_else(|_| generate_thumbnail(&original_bytes))
            .map_err(|e| {
                tracing::warn!(
                    url = url,
                    error = %e,
                    "Failed to generate thumbnail for image"
                );
                e
            })
            .ok()
        {
            if let Err(e) = self.store_thumbnail(&processed.id, &thumbnail).await {
                tracing::warn!(
                    image_id = %processed.id,
                    error = %e,
                    "Failed to store image thumbnail"
                );
            }
        }

        Ok(processed)
    }

    async fn remove_background_http(&self, endpoint: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let part = reqwest::multipart::Part::bytes(bytes.to_vec())
            .file_name("image")
            .mime_str("application/octet-stream")?;
        let form = reqwest::multipart::Form::new().part("file", part);

        let response = self
            .client
            .post(endpoint)
            .multipart(form)
            .send()
            .await
            .context("Failed to call rembg server")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "rembg server returned status {}",
                response.status()
            ));
        }

        let png_bytes = response.bytes().await?.to_vec();
        if png_bytes.is_empty() {
            return Err(anyhow::anyhow!("rembg server returned empty body"));
        }

        Ok(png_bytes)
    }

    async fn fetch_cached(&self, id: &str) -> Result<Option<ProcessedImage>> {
        let row = sqlx::query(
            "SELECT url, image_bytes, content_type, is_transparent FROM images WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ProcessedImage {
            url: row.get::<String, _>("url"),
            id: id.to_string(),
            bytes: row.get::<Vec<u8>, _>("image_bytes"),
            content_type: row.get::<Option<String>, _>("content_type"),
            is_transparent: row.get::<i64, _>("is_transparent") != 0,
            from_cache: true,
        }))
    }

    async fn store_cached(&self, result: &ProcessedImage) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO images (id, url, image_bytes, content_type, is_transparent)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                url = excluded.url,
                image_bytes = excluded.image_bytes,
                content_type = excluded.content_type,
                is_transparent = excluded.is_transparent
            "#,
        )
        .bind(&result.id)
        .bind(&result.url)
        .bind(&result.bytes)
        .bind(&result.content_type)
        .bind(if result.is_transparent { 1 } else { 0 })
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn store_thumbnail(&self, image_id: &str, thumbnail: &Thumbnail) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO image_thumbnails (image_id, thumb_bytes, content_type, width, height)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(image_id) DO UPDATE SET
                thumb_bytes = excluded.thumb_bytes,
                content_type = excluded.content_type,
                width = excluded.width,
                height = excluded.height
            "#,
        )
        .bind(image_id)
        .bind(&thumbnail.bytes)
        .bind(&thumbnail.content_type)
        .bind(thumbnail.width as i64)
        .bind(thumbnail.height as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn process_missing_thumbnails(&self) -> Result<usize> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT id
            FROM images
            WHERE id NOT IN (SELECT image_id FROM image_thumbnails)
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut tasks = FuturesUnordered::new();
        for (image_id,) in rows {
            let processor = self.clone();
            let permit = self.semaphore.clone().acquire_owned().await?;
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                processor.ensure_thumbnail_for_id(&image_id).await
            }));
        }

        let mut created = 0usize;
        while let Some(joined) = tasks.next().await {
            let did_create = joined.context("Thumbnail task panicked")??;
            if did_create {
                created += 1;
            }
        }

        Ok(created)
    }

    async fn ensure_thumbnail_for_id(&self, image_id: &str) -> Result<bool> {
        let exists = sqlx::query("SELECT 1 FROM image_thumbnails WHERE image_id = ? LIMIT 1")
            .bind(image_id)
            .fetch_optional(&self.pool)
            .await?;
        if exists.is_some() {
            return Ok(false);
        }

        let row = sqlx::query("SELECT image_bytes FROM images WHERE id = ?")
            .bind(image_id)
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else {
            return Ok(false);
        };

        let bytes: Vec<u8> = row.get("image_bytes");
        match generate_thumbnail(&bytes) {
            Ok(thumbnail) => {
                self.store_thumbnail(image_id, &thumbnail).await?;
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(
                    image_id = %image_id,
                    error = %e,
                    "Failed to generate thumbnail from cached image"
                );
                Ok(false)
            }
        }
    }
}

fn normalize_rembg_endpoint(endpoint: String) -> String {
    let mut endpoint = if endpoint.trim().is_empty() {
        DEFAULT_REMBG_ENDPOINT.to_string()
    } else {
        endpoint
    };

    if endpoint.ends_with(REMBG_REMOVE_PATH) {
        return endpoint;
    }

    if endpoint.ends_with('/') {
        endpoint.pop();
    }

    if endpoint.ends_with(REMBG_REMOVE_PATH) {
        endpoint
    } else {
        format!("{endpoint}{REMBG_REMOVE_PATH}")
    }
}

fn hash_url(url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    to_hex(&digest)
}

/// Public helper to compute the image cache ID for a URL.
pub fn image_id_for_url(url: &str) -> String {
    hash_url(url)
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

fn decode_image(bytes: &[u8]) -> Result<DynamicImage> {
    image::load_from_memory(bytes).context("Failed to decode image bytes")
}

fn encode_png(image: &DynamicImage) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
        .context("Failed to encode PNG")?;
    Ok(out)
}

fn generate_thumbnail(bytes: &[u8]) -> Result<Thumbnail> {
    let image = decode_image(bytes)?;
    let thumb = image.thumbnail(THUMBNAIL_SIZE, THUMBNAIL_SIZE);
    let (width, height) = thumb.dimensions();
    let mut out = Vec::new();
    thumb
        .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
        .context("Failed to encode thumbnail PNG")?;
    Ok(Thumbnail {
        bytes: out,
        content_type: "image/png".to_string(),
        width,
        height,
    })
}

fn fallback_result(
    url: &str,
    id: &str,
    bytes: &[u8],
    content_type: Option<String>,
) -> ProcessedImage {
    ProcessedImage {
        url: url.to_string(),
        id: id.to_string(),
        bytes: bytes.to_vec(),
        content_type,
        is_transparent: false,
        from_cache: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    const TEST_URL: &str = "https://i5.walmartimages.com/seo/POKEMON-ME2-5-ASCENDED-HEROES-TECH-STICKER-3PK-BLISTER_12e39cb3-906f-4042-8824-36443504322b.a638e9db35530ad4ae5d376d4f8b3db8.jpeg";

    #[tokio::test]
    async fn process_batch_downloads_and_caches_images() {
        let db = Database::in_memory().await.expect("db init");
        db.run_migrations().await.expect("migrations");

        let processor = ImageProcessor::new(db.pool().clone(), Arc::new(NoopRemover))
            .await
            .expect("processor");

        let first = processor
            .process_batch(vec![TEST_URL.to_string()])
            .await
            .expect("process first");

        assert_eq!(first.len(), 1);
        let first_result = &first[0];
        assert!(!first_result.bytes.is_empty(), "image bytes should be non-empty");
        assert_eq!(first_result.url, TEST_URL);
        assert!(!first_result.from_cache);

        let second = processor
            .process_batch(vec![TEST_URL.to_string()])
            .await
            .expect("process second");

        assert_eq!(second.len(), 1);
        let second_result = &second[0];
        assert!(second_result.from_cache, "second fetch should hit cache");
        assert_eq!(second_result.bytes, first_result.bytes);

        let image_id = image_id_for_url(TEST_URL);
        let row = sqlx::query(
            "SELECT thumb_bytes, width, height FROM image_thumbnails WHERE image_id = ?",
        )
        .bind(&image_id)
        .fetch_optional(db.pool())
        .await
        .expect("thumbnail query");

        let row = row.expect("thumbnail should be stored");
        let thumb_bytes: Vec<u8> = row.get("thumb_bytes");
        let width: i64 = row.get("width");
        let height: i64 = row.get("height");

        assert!(!thumb_bytes.is_empty(), "thumbnail bytes should be non-empty");
        assert!(width > 0 && width <= THUMBNAIL_SIZE as i64);
        assert!(height > 0 && height <= THUMBNAIL_SIZE as i64);
    }

    #[test]
    fn default_rembg_endpoint_is_localhost() {
        assert_eq!(DEFAULT_REMBG_ENDPOINT, "http://127.0.0.1:5000");
    }

    #[test]
    fn rembg_endpoint_normalization_appends_path() {
        let base = "http://127.0.0.1:5000".to_string();
        assert_eq!(
            normalize_rembg_endpoint(base),
            "http://127.0.0.1:5000/api/remove"
        );
    }
}
