# 指标计算方式与合理性分析

## 1. 每日UV/PV（daily_uv_pv.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| uv | `countDistinct(user_id)` | 当日独立用户数 |
| pv | `count(when(behavior=="pv", 1))` | 当日浏览总次数 |
| total_behavior | `count(*)` | 当日所有行为总数 |

**合理性**: ✅ uv < pv 是正常的（一个用户可以多次浏览）。

---

## 2. 日留存率（retention_rate.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| new_users | 每日首次出现的用户数 | 按 `min(date)` 计算 |
| retained_dayN | 第N天仍活跃的独立用户数 | `day_diff == N` |
| retention_rate_dayN | retained_dayN / new_users × 100 | 留存率百分比 |

**合理性**: ✅ 数据集仅9天，7日留存率会偏低是正常的。留存计算基于首次访问日期，不是首次购买日期。

---

## 3. RFM用户价值分层（rfm_segments.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| recency | `datediff(max_date, 最后购买日期)` | 距今天数 |
| frequency | `count(*)` where behavior=buy | 购买次数 |
| r_score | 按阈值 [1,3,5,7] 打分 1-5 | 越近越高 |
| f_score | 按阈值 [1,2,4,9] 打分 1-5 | 越频繁越高 |
| total_score | R×0.4 + F×0.6 | 加权总分 |
| 分层 | ≥4.0高价值, ≥3.0潜力, ≥2.0一般, ≥1.0预流失, <1.0流失 | 5个层级 |

**合理性**: ✅ 只分析购买用户。R和F权重可在 config.yaml 调整。数据集仅9天，recency 值普遍较小。

---

## 4. 商品转化率（item_cvr_top.json / category_cvr.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| pv | `countDistinct(user_id) where behavior=pv` | 浏览独立用户数 |
| fav | `countDistinct(user_id) where behavior=fav` | 收藏独立用户数 |
| cart | `countDistinct(user_id) where behavior=cart` | 加购独立用户数 |
| buy | `countDistinct(user_id) where behavior=buy` | 购买独立用户数 |
| interest_uv | pv + cart + fav | 感兴趣的独立用户总数 |
| pv_to_buy_rate | buy / interest_uv × 100 | 购买转化率 |

**合理性**: ✅ 已修复为独立用户数。仍有少量商品 CVR > 100%，这是正常的：用户可通过收藏夹/购物车直接购买，不需要先浏览。此时 buy_uv 可能大于 pv_uv。

**CVT > 100% 的三种合法场景**:
1. 用户通过收藏夹回访直接购买（无本次浏览）
2. 用户通过购物车直接结算（无本次浏览）
3. 用户在其他渠道（如搜索结果页）直接购买

---

## 5. 转化漏斗（conversion_funnel.json）

| 阶段 | 计算方式 | 说明 |
|------|---------|------|
| 浏览(pv) | 有浏览行为的独立用户数 | 基数 |
| 加购/收藏 | 有浏览**且**有加购/收藏行为的用户 | 严格递进 |
| 购买 | 有浏览**且**加购/收藏**且**购买的用户 | 严格递进 |

**合理性**: ✅ 已修复为严格递进漏斗。每阶段是前一阶段的子集，百分比单调递减。

**注意**: 漏斗只统计"三步全通"的用户。直接购买（无浏览）或仅浏览后直接购买（无加购/收藏）的用户不在漏斗中，因此购买阶段人数会少于总体购买人数。这是**设计如此**，不是 bug。

---

## 6. 用户行为路径（behavior_paths.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| path_segment | `prev_behavior → behavior` | 两步路径 |
| path3 | `prev2 → prev1 → behavior` | 三步路径 |
| 会话定义 | 两次行为间隔 > 30 分钟视为新会话 | 可在 config 调整 |

**合理性**: ✅ 已修复为基于会话内路径，不会跨会话拼接。

---

## 7. 时间维度分析（hourly_cvr.json / weekday_vs_weekend.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| pv_to_buy_rate | buy_count / pv_count × 100 | 每小时行为转化率 |
| uv_cvr | buy_uv / pv_uv × 100 | 每小时用户转化率 |
| buy_rate | buy / pv × 100 | 工作日/周末购买率 |

**合理性**: ✅ 已修复为动态计算工作日/周末天数。同时提供行为级和用户级两种转化率，互相验证。

---

## 8. 用户生命周期（user_lifecycle.json / first_purchase_gap.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| new_users | 当日首次出现的用户数 | 新用户 |
| returning_users | total - new | 回访用户 |
| gap_days | 首次购买日期 - 首次浏览日期 | 首购间隔 |

**合理性**: ✅ 新用户比例逐日下降是正常的（数据集只有9天，新用户在第一天最多）。

---

## 9. 商品/类目热度（item_hot_rank.json / category_hot_rank.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| pv/fav/cart/buy | `count(when(behavior==X, 1))` | 各行为总次数 |
| total | pv + fav + cart + buy | 热度得分 |

**合理性**: ✅ 热度排行用行为总次数（不是独立用户数），因为热度应该反映"被操作的频率"。

---

## 10. 行为分布概览（behavior_distribution.json）

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| count | 各行为的总次数 | 绝对值 |
| percentage | count / total × 100 | 占比 |

**合理性**: ✅ PV 占比最大（约 90%+），购买占比最小，符合电商行为金字塔。
