// TypeScript interfaces matching Rust Serialize structs.
// Field names are snake_case to match serde defaults.

export interface ItemViewModel {
  name: string;
  quantity: number;
  status: string;
  image_id: string | null;
  image_url: string | null;
}

export interface OrderViewModel {
  id: string;
  order_date: string;
  order_date_raw: string;
  shipped_date: string | null;
  status: string;
  total_cost: string | null;
  items: ItemViewModel[];
  tracking_number: string | null;
  carrier: string | null;
  recipient: string | null;
  thumbnail_id: string | null;
  thumbnail_url: string | null;
  cancel_reason: string | null;
}

export interface StatusCounts {
  confirmed: number;
  shipped: number;
  delivered: number;
  canceled: number;
  partially_canceled: number;
}

export interface AccountViewModel {
  id: number;
  email: string;
  display_name: string | null;
  profile_picture_url: string | null;
  order_count: number;
  last_sync_at: string | null;
}

export interface DashboardData {
  orders: OrderViewModel[];
  total_orders: number;
  pending_emails: number;
  status_counts: StatusCounts;
  last_updated: string;
  accounts: AccountViewModel[];
  selected_account_id: number | null;
}

export interface PaginatedOrders {
  orders: OrderViewModel[];
  has_more: boolean;
  next_cursor: string | null;
  total_count: number;
}

export interface DashboardDataV2 {
  paginated_orders: PaginatedOrders;
  status_counts: StatusCounts;
  accounts: AccountViewModel[];
  selected_account_id: number | null;
  pending_emails: number;
  last_updated: string;
}

export interface TrackingBreakdown {
  label_created: number;
  in_transit: number;
  out_for_delivery: number;
  delivered: number;
  exception: number;
  available_for_pickup: number;
  unknown: number;
}

export interface AggregateStats {
  total_orders: number;
  total_spent: number;
  total_quantity: number;
  tracking_breakdown: TrackingBreakdown;
}

export interface VirtualScrollState {
  scrollTop: number;
  containerHeight: number;
  itemHeight: number;
  visibleStart: number;
  visibleEnd: number;
  totalItems: number;
}

export interface SyncResult {
  success: boolean;
  emails_synced: number;
  orders_processed: number;
  errors: string[];
  message: string;
}

export interface TrackingEventResponse {
  time: string | null;
  description: string;
  location: string | null;
}

export interface TrackingStatusResponse {
  tracking_number: string;
  carrier: string;
  state: string;
  state_display: string;
  state_description: string | null;
  is_delivered: boolean;
  delivery_date: string | null;
  estimated_delivery: string | null;
  last_fetched_at: string;
  events: TrackingEventResponse[];
}

export interface NewEmailCheck {
  total_new: number;
  total_pending: number;
}

export interface SyncProgress {
  stage: number;
  total_stages: number;
  label: string;
  detail: string;
}

export type StatusFilter = 'all' | 'confirmed' | 'shipped' | 'delivered' | 'canceled' | 'partially_canceled';
export type SortMode = 'date' | 'price' | 'status' | 'quantity' | 'items';
export type SortDirection = 'asc' | 'desc';

export interface DateRangeParams {
  startDate: string | null;
  endDate: string | null;
}

export interface TimelineEvent {
  time: string;
  description: string;
  location: string | null;
}

export interface UpcomingDelivery {
  order_id: string;
  tracking_number: string;
  carrier: string;
  estimated_delivery: string;
  state: string;
  state_display: string;
  item_name: string | null;
  item_count: number;
  image_id: string | null;
}
