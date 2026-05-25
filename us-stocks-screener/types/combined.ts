export type TierKey = "超大型" | "巨型" | "大型" | "中大型" | "中型" | "小型";

export interface CombinedStock {
  ticker:            string;
  name:              string;
  sector:            string;
  tier:              TierKey;
  currentCap:        number;
  maStrength:        number;
  ma3:               number;
  ma10:              number;
  ma30:              number;
  ma60:              number;
  crossoverCount:    number;
  uniqueLosers:      number;
  lastCrossoverDate: string;
}

export interface CombinedTier {
  tier:   TierKey;
  stocks: CombinedStock[];
}
