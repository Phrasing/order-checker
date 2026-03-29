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
use std::path::Path;
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

/// Background remover powered by the rembg-rs crate (native ONNX inference).
pub struct RembgRemover {
    manager: std::sync::Mutex<rembg_rs::manager::ModelManager>,
}

/// Result of ONNX runtime verification.
#[derive(Debug, Clone)]
pub enum OnnxStatus {
    /// ONNX runtime is working correctly
    Working,
    /// ONNX runtime failed - user should install VC++ Redistributable
    NeedsVcRedist(String),
}

impl RembgRemover {
    pub fn new(model_path: &Path) -> Result<Self> {
        let path = model_path.to_path_buf();

        // Catch panics during ONNX model loading (can crash on systems without VC++ Redistributable)
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rembg_rs::manager::ModelManager::from_file(&path)
        }));

        match result {
            Ok(Ok(manager)) => Ok(Self {
                manager: std::sync::Mutex::new(manager),
            }),
            Ok(Err(err)) => Err(anyhow::anyhow!(
                "ONNX model load error: {}. Install Visual C++ Redistributable: https://aka.ms/vs/17/release/vc_redist.x64.exe",
                err
            )),
            Err(_) => Err(anyhow::anyhow!(
                "ONNX runtime crashed during initialization. Install Visual C++ Redistributable: https://aka.ms/vs/17/release/vc_redist.x64.exe"
            )),
        }
    }

    /// Verify ONNX inference works by running a small test.
    /// Returns OnnxStatus indicating whether VC++ Redistributable is needed.
    pub fn verify_working(&self) -> OnnxStatus {
        // Create a tiny 8x8 test image (minimum size for rembg)
        let test_img = DynamicImage::new_rgb8(8, 8);

        match self.remove_background(test_img) {
            Ok(_) => OnnxStatus::Working,
            Err(err) => OnnxStatus::NeedsVcRedist(err.to_string()),
        }
    }
}

impl BackgroundRemover for RembgRemover {
    fn remove_background(&self, image: DynamicImage) -> Result<DynamicImage> {
        let mut mgr = self
            .manager
            .lock()
            .map_err(|e| anyhow::anyhow!("Model manager lock poisoned: {}", e))?;

        let options = rembg_rs::options::RemovalOptions::default();

        // Catch panics during ONNX inference (can crash on systems without VC++ Redistributable)
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rembg_rs::rembg::rembg(&mut mgr, image.clone(), &options)
        }));

        match result {
            Ok(Ok(rembg_result)) => {
                let (rgba, _mask) = rembg_result.into_parts();
                Ok(DynamicImage::ImageRgba8(rgba))
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("rembg failed: {}", err)),
            Err(_) => Err(anyhow::anyhow!(
                "ONNX inference crashed. Install Visual C++ Redistributable: https://aka.ms/vs/17/release/vc_redist.x64.exe"
            )),
        }
    }
}

const MODEL_FILENAME: &str = "silueta.onnx";
const MODEL_URL: &str =
    "https://github.com/danielgatis/rembg/releases/download/v0.0.0/silueta.onnx";
const THUMBNAIL_SIZE: u32 = 112;

const ORT_VERSION: &str = "1.22.0";
const ORT_DLL_FILENAME: &str = "onnxruntime.dll";
#[cfg(target_os = "windows")]
const ORT_DOWNLOAD_URL: &str = "https://github.com/microsoft/onnxruntime/releases/download/v1.22.0/onnxruntime-win-x64-1.22.0.zip";

struct Thumbnail {
    bytes: Vec<u8>,
    content_type: String,
    width: u32,
    height: u32,
}

/// Processor for downloading and caching product images.
#[derive(Clone)]
pub struct ImageProcessor {
    pool: SqlitePool,
    client: Client,
    semaphore: Arc<Semaphore>,
    remover: Arc<dyn BackgroundRemover>,
    /// True if using a real background remover (not NoopRemover)
    has_real_remover: bool,
}

impl ImageProcessor {
    /// Create a processor with local background removal via rembg-rs.
    /// Downloads the ONNX model on first use if not present in `models_dir`.
    /// Returns (ImageProcessor, Option<OnnxStatus>) - the status indicates if VC++ is needed.
    pub async fn new(pool: SqlitePool, models_dir: &Path) -> Result<(Self, Option<OnnxStatus>)> {
        // Enable WAL for better concurrency during large syncs.
        sqlx::query("PRAGMA journal_mode=WAL;")
            .execute(&pool)
            .await?;

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()?;

        // Ensure ONNX Runtime DLL is available (downloads ~70 MB on first run)
        ensure_ort_runtime_impl(models_dir, &client).await?;

        // Download ONNX model if missing (~43 MB on first run)
        let model_path = ensure_model(models_dir, &client).await?;

        // Create local background remover with pre-flight verification
        let (remover, has_real_remover, onnx_status): (Arc<dyn BackgroundRemover>, bool, Option<OnnxStatus>) =
            match RembgRemover::new(&model_path) {
                Ok(rembg) => {
                    // Verify ONNX actually works with a test inference
                    let status = rembg.verify_working();
                    match &status {
                        OnnxStatus::Working => {
                            tracing::info!("ONNX background remover initialized and verified");
                            (Arc::new(rembg), true, None)
                        }
                        OnnxStatus::NeedsVcRedist(err) => {
                            tracing::warn!(
                                "ONNX verification failed: {}. Using NoopRemover.",
                                err
                            );
                            (Arc::new(NoopRemover), false, Some(status))
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to init background remover: {}. Using NoopRemover.",
                        err
                    );
                    let status = OnnxStatus::NeedsVcRedist(err.to_string());
                    (Arc::new(NoopRemover), false, Some(status))
                }
            };

        let max_concurrency = std::cmp::max(1, num_cpus::get() / 2);
        let max_concurrency = std::cmp::min(8, max_concurrency);
        Ok((
            Self {
                pool,
                client,
                semaphore: Arc::new(Semaphore::new(max_concurrency)),
                remover,
                has_real_remover,
            },
            onnx_status,
        ))
    }

    /// Create a processor without background removal (thumbnails / download only).
    pub async fn new_noop(pool: SqlitePool) -> Result<Self> {
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
            remover: Arc::new(NoopRemover),
            has_real_remover: false,
        })
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

        // Download at reduced resolution when possible (saves bandwidth + faster processing)
        let download_url = optimize_image_download_url(url);
        let response = self
            .client
            .get(download_url.as_str())
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

        // Run background removal on a blocking thread (CPU-bound ONNX inference)
        let remover = Arc::clone(&self.remover);
        let bytes_for_removal = original_bytes.clone();
        let has_real_remover = self.has_real_remover;
        let processed = match tokio::task::spawn_blocking(move || {
            let image = decode_image(&bytes_for_removal)?;
            let removed = remover.remove_background(image)?;
            encode_png(&removed)
        })
        .await?
        {
            Ok(png_bytes) => ProcessedImage {
                url: url.to_string(),
                id: id.clone(),
                bytes: png_bytes,
                content_type: Some("image/png".to_string()),
                // Only mark as transparent if we used a real background remover
                is_transparent: has_real_remover,
                from_cache: false,
            },
            Err(err) => {
                tracing::warn!(
                    url = url,
                    error = %err,
                    "Background removal failed; falling back to original image"
                );
                fallback_result(url, &id, &original_bytes, content_type.clone())
            }
        };

        self.store_cached(&processed).await?;

        if let Ok(thumbnail) = generate_thumbnail(&processed.bytes)
            .or_else(|_| generate_thumbnail(&original_bytes))
            .map_err(|e| {
                tracing::warn!(
                    url = url,
                    error = %e,
                    "Failed to generate thumbnail for image"
                );
                e
            })
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

    /// Count images that don't have transparent backgrounds.
    pub async fn count_non_transparent_images(&self) -> Result<usize> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM images WHERE is_transparent = 0"
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0 as usize)
    }

    /// Reprocess all non-transparent images with background removal.
    /// Returns the number of images successfully reprocessed.
    pub async fn reprocess_non_transparent_images(&self) -> Result<usize> {
        let rows: Vec<(String, String, Vec<u8>)> = sqlx::query_as(
            "SELECT id, url, image_bytes FROM images WHERE is_transparent = 0"
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        tracing::info!("Reprocessing {} non-transparent images with background removal", rows.len());

        let mut reprocessed = 0usize;
        let mut tasks = FuturesUnordered::new();

        for (image_id, url, original_bytes) in rows {
            let processor = self.clone();
            let permit = self.semaphore.clone().acquire_owned().await?;
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                processor.reprocess_single_image(&image_id, &url, &original_bytes).await
            }));
        }

        while let Some(joined) = tasks.next().await {
            match joined {
                Ok(Ok(true)) => reprocessed += 1,
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    tracing::warn!("Failed to reprocess image: {}", e);
                }
                Err(e) => {
                    tracing::warn!("Reprocess task panicked: {}", e);
                }
            }
        }

        tracing::info!("Reprocessed {} images with transparent backgrounds", reprocessed);
        Ok(reprocessed)
    }

    async fn reprocess_single_image(&self, image_id: &str, url: &str, original_bytes: &[u8]) -> Result<bool> {
        // Run background removal on the original image
        let remover = Arc::clone(&self.remover);
        let bytes_for_removal = original_bytes.to_vec();

        let processed = match tokio::task::spawn_blocking(move || {
            let image = decode_image(&bytes_for_removal)?;
            let removed = remover.remove_background(image)?;
            encode_png(&removed)
        })
        .await?
        {
            Ok(png_bytes) => {
                // Update the image in the database
                sqlx::query(
                    "UPDATE images SET image_bytes = ?, content_type = 'image/png', is_transparent = 1 WHERE id = ?"
                )
                .bind(&png_bytes)
                .bind(image_id)
                .execute(&self.pool)
                .await?;

                // Regenerate thumbnail with new transparent image
                if let Ok(thumbnail) = generate_thumbnail(&png_bytes) {
                    let _ = self.store_thumbnail(image_id, &thumbnail).await;
                }

                tracing::debug!("Reprocessed image {} with transparency", image_id);
                true
            }
            Err(err) => {
                tracing::warn!(
                    url = url,
                    error = %err,
                    "Background removal failed during reprocessing"
                );
                false
            }
        };

        Ok(processed)
    }

    /// Check if this processor has a working background remover (not NoopRemover).
    pub fn has_working_remover(&self) -> bool {
        self.has_real_remover
    }
}

async fn ensure_model(models_dir: &Path, client: &Client) -> Result<std::path::PathBuf> {
    let model_path = models_dir.join(MODEL_FILENAME);
    if model_path.exists() {
        return Ok(model_path);
    }

    std::fs::create_dir_all(models_dir)
        .with_context(|| format!("Failed to create models dir: {}", models_dir.display()))?;

    tracing::info!(
        "Downloading background removal model ({})...",
        MODEL_FILENAME
    );

    let response = client
        .get(MODEL_URL)
        .send()
        .await
        .context("Failed to download ONNX model")?
        .error_for_status()
        .context("ONNX model download returned error status")?;

    let bytes = response
        .bytes()
        .await
        .context("Failed to read ONNX model response body")?;

    std::fs::write(&model_path, &bytes)
        .with_context(|| format!("Failed to write model to {}", model_path.display()))?;

    tracing::info!(
        "Model downloaded ({:.1} MB) to {}",
        bytes.len() as f64 / 1_048_576.0,
        model_path.display()
    );

    Ok(model_path)
}

/// Download and set up the ONNX Runtime DLL so `ort` finds the correct version.
#[cfg(target_os = "windows")]
async fn ensure_ort_runtime_impl(models_dir: &Path, client: &Client) -> Result<std::path::PathBuf> {
    let dll_path = models_dir.join(ORT_DLL_FILENAME);
    if dll_path.exists() {
        std::env::set_var("ORT_DYLIB_PATH", &dll_path);
        return Ok(dll_path);
    }

    std::fs::create_dir_all(models_dir)
        .with_context(|| format!("Failed to create models dir: {}", models_dir.display()))?;

    tracing::info!("Downloading ONNX Runtime v{}...", ORT_VERSION);

    let zip_path = models_dir.join(format!("onnxruntime-win-x64-{}.zip", ORT_VERSION));
    let response = client
        .get(ORT_DOWNLOAD_URL)
        .send()
        .await
        .context("Failed to download ONNX Runtime")?
        .error_for_status()
        .context("ONNX Runtime download returned error status")?;

    let bytes = response
        .bytes()
        .await
        .context("Failed to read ONNX Runtime response body")?;

    std::fs::write(&zip_path, &bytes)
        .with_context(|| format!("Failed to write zip to {}", zip_path.display()))?;

    tracing::info!(
        "Downloaded ONNX Runtime ({:.1} MB), extracting...",
        bytes.len() as f64 / 1_048_576.0
    );

    // Extract the DLL directly from the zip archive using the zip crate
    let dll_bytes = {
        let zip_file = std::fs::File::open(&zip_path)
            .with_context(|| format!("Failed to open zip file: {}", zip_path.display()))?;
        let mut archive = zip::ZipArchive::new(zip_file)
            .context("Failed to read ONNX Runtime zip archive")?;

        // Find and extract the DLL from the archive
        let dll_entry_path = format!("onnxruntime-win-x64-{}/lib/{}", ORT_VERSION, ORT_DLL_FILENAME);
        let mut dll_entry = archive
            .by_name(&dll_entry_path)
            .with_context(|| format!("DLL not found in archive at path: {}", dll_entry_path))?;

        let mut bytes = Vec::with_capacity(dll_entry.size() as usize);
        std::io::Read::read_to_end(&mut dll_entry, &mut bytes)
            .context("Failed to read DLL from zip archive")?;
        bytes
        };

    std::fs::write(&dll_path, &dll_bytes)
        .with_context(|| format!("Failed to write DLL to {}", dll_path.display()))?;

    // Clean up zip file
    std::fs::remove_file(&zip_path).ok();

    std::env::set_var("ORT_DYLIB_PATH", &dll_path);
    tracing::info!(
        "ONNX Runtime v{} installed to {}",
        ORT_VERSION,
        dll_path.display()
    );

    Ok(dll_path)
}

#[cfg(not(target_os = "windows"))]
async fn ensure_ort_runtime_impl(_models_dir: &Path, _client: &Client) -> Result<std::path::PathBuf> {
    anyhow::bail!("Automatic ONNX Runtime download is only supported on Windows. Set ORT_DYLIB_PATH manually.")
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

/// Request a smaller image from Walmart CDN when the URL supports it.
/// The thumbnail is 112×112 so 300px is plenty for rembg + thumbnail generation.
/// The image cache ID is computed from the *original* URL so caching is unaffected.
fn optimize_image_download_url(url: &str) -> String {
    if url.contains("walmartimages.com") && !url.contains("odnWidth") {
        let sep = if url.contains('?') { "&" } else { "?" };
        format!("{url}{sep}odnWidth=300&odnHeight=300&odnBg=FFFFFF")
    } else {
        url.to_string()
    }
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

        let processor = ImageProcessor::new_noop(db.pool().clone())
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
}
