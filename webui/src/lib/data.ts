// Data loaders — fetch JSON from /data and cache in-memory.

export type D1Row = {
  language: string;
  year_month: string;
  repo_count: number;
  total_bytes: number;
  commit_count: number;
};

export type D1Cluster = {
  language: string;
  cluster: number;
  avg_repo_growth: number;
  commit_volatility: number;
  repo_range_ratio: number;
  avg_repo_count: number;
  avg_commit_count: number;
};

export type D2Row = {
  language: string;
  year_month: string;
  actual: number | null;
  prophet_yhat: number | null;
  prophet_lower: number | null;
  prophet_upper: number | null;
  arima_yhat: number | null;
  is_forecast: boolean;
};

export type D3Edge = {
  from_tech: string;
  to_tech: string;
  migration_count: number;
  years_active: number;
  first_year: number;
  last_year: number;
};

export type D3Node = {
  tech: string;
  incoming: number;
  outgoing: number;
  net_flow: number;
  pagerank: number;
};

export type D4Dev = {
  dev_short: string;
  pagerank: number;
  hub_score: number;
  auth_score: number;
  degree: number;
  total_commits: number;
};

export type D5Community = {
  community_id: number;
  size: number;
  avg_pagerank: number;
  max_pagerank: number;
  avg_degree: number;
  top_orgs: string;
};

export type D5BridgeDev = {
  dev_short: string;
  community_id: number;
  pagerank: number;
  degree: number;
};

const cache = new Map<string, unknown>();

async function load<T>(path: string): Promise<T> {
  if (cache.has(path)) return cache.get(path) as T;
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}: ${res.status}`);
  const json = (await res.json()) as T;
  cache.set(path, json);
  return json;
}

export const loadD1 = () =>
  load<{ top_langs: string[]; rows: D1Row[] }>("/data/d1_monthly.json");
export const loadD1Clusters = () => load<D1Cluster[]>("/data/d1_clusters.json");
export const loadD2 = () => load<D2Row[]>("/data/d2_forecasts.json");
export const loadD3 = () =>
  load<{ edges: D3Edge[]; nodes: D3Node[] }>("/data/d3_migration.json");
export const loadD4 = () => load<D4Dev[]>("/data/d4_pagerank.json");
export const loadD5 = () =>
  load<{
    communities: D5Community[];
    bridge_devs: D5BridgeDev[];
    total_developers: number;
    total_communities: number;
    largest_community: number;
    singleton_communities: number;
  }>("/data/d5_communities.json");
