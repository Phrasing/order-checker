import type {
  OrderViewModel,
  AccountViewModel,
  StatusFilter,
  SortMode,
  TrackingStatusResponse,
  VirtualScrollState,
} from './types';

// Mutable state — exported as live bindings with setter functions
// (ES module exports are read-only from other modules, so setters are needed)

export let allOrders: OrderViewModel[] = [];
export let allAccounts: AccountViewModel[] = [];
export let currentFilter: StatusFilter = 'all';
export let currentSort: SortMode = 'date';
export let currentAccountId: number | null = null;
export let currentDatePreset = '0';
export let currentSearchQuery = '';
export let newEmailCount = 0;
export let fetchSinceDate: string | null = localStorage.getItem('fetchSinceDate');
export let isSyncing = false;
export let currentRenderedOrders: OrderViewModel[] = [];
export let expandedOrderId: string | null = null;
export let lastUpdatedText = '';

// Pagination state for virtual scrolling
export let loadedOrders: OrderViewModel[] = [];
export let hasMoreOrders = false;
export let nextCursor: string | null = null;
export let isLoadingMore = false;
export let virtualScrollState: VirtualScrollState = {
  scrollTop: 0,
  containerHeight: 0,
  itemHeight: 120,
  visibleStart: 0,
  visibleEnd: 0,
  totalItems: 0,
};

// Memoization cache for expensive operations
export const productSummaryCache = new Map<string, string>();

export function setAllOrders(v: OrderViewModel[]): void { allOrders = v; }
export function setAllAccounts(v: AccountViewModel[]): void { allAccounts = v; }
export function setCurrentFilter(v: StatusFilter): void { currentFilter = v; }
export function setCurrentSort(v: SortMode): void { currentSort = v; }
export function setCurrentAccountId(v: number | null): void { currentAccountId = v; }
export function setCurrentDatePreset(v: string): void { currentDatePreset = v; }
export function setCurrentSearchQuery(v: string): void { currentSearchQuery = v; }
export function setNewEmailCount(v: number): void { newEmailCount = v; }
export function setFetchSinceDate(v: string | null): void { fetchSinceDate = v; }
export function setIsSyncing(v: boolean): void { isSyncing = v; }
export function setCurrentRenderedOrders(v: OrderViewModel[]): void { currentRenderedOrders = v; }
export function setExpandedOrderId(v: string | null): void { expandedOrderId = v; }
export function setLastUpdatedText(v: string): void { lastUpdatedText = v; }

// Pagination setters
export function setLoadedOrders(v: OrderViewModel[]): void { loadedOrders = v; }
export function appendLoadedOrders(v: OrderViewModel[]): void {
  loadedOrders = [...loadedOrders, ...v];
}
export function setHasMoreOrders(v: boolean): void { hasMoreOrders = v; }
export function setNextCursor(v: string | null): void { nextCursor = v; }
export function setIsLoadingMore(v: boolean): void { isLoadingMore = v; }
export function setVirtualScrollState(v: Partial<VirtualScrollState>): void {
  virtualScrollState = { ...virtualScrollState, ...v };
}

// Caches
export const trackingCache = new Map<string, TrackingStatusResponse>();
export const thumbnailCache = new Map<string, string>();
export let orderThumbObserver: IntersectionObserver | null = null;
export function setOrderThumbObserver(v: IntersectionObserver | null): void { orderThumbObserver = v; }

// Constants

export const statusPriority: Record<string, number> = {
  shipped: 1,
  confirmed: 2,
  partially_canceled: 3,
  delivered: 4,
  canceled: 5,
};

export const trackingUrls: Record<string, (num: string) => string> = {
  fedex: (num) => `https://www.fedex.com/fedextrack/?trknbr=${num}`,
  ups: (num) => `https://www.ups.com/track?tracknum=${num}`,
  usps: (num) => `https://tools.usps.com/go/TrackConfirmAction?tLabels=${num}`,
};

export const statusLabels: Record<string, string> = {
  confirmed: 'Confirmed',
  shipped: 'Shipped',
  delivered: 'Delivered',
  canceled: 'Canceled',
  partially_canceled: 'Partial',
};

export const trackingStateColors: Record<string, string> = {
  label_created: 'badge-gray',
  in_transit: 'badge-blue',
  out_for_delivery: 'badge-yellow',
  delivered: 'badge-green',
  exception: 'badge-red',
  available_for_pickup: 'badge-purple',
  unknown: 'badge-gray',
};

export const filterTitles: Record<string, string> = {
  all: 'All Orders',
  confirmed: 'Confirmed Orders',
  shipped: 'Shipped Orders',
  delivered: 'Delivered Orders',
  canceled: 'Canceled Orders',
  partially_canceled: 'Partially Canceled',
};
