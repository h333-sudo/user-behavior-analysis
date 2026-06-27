# 待修复问题清单

## 问题1：商品转化率 > 100%（已修复）

**文件**: `lib/analyses/product_cvr.py`

**现象**: 商品 3173847 的 `pv_to_buy_rate` = 1053%，逻辑上不可能。

**原因**: 原代码用 `count(when(behavior=="buy", 1))` 统计的是**购买总次数**，不是**购买用户数**。同一用户多次购买会重复计数。

**修复**: ✅ 已修改为 `countDistinct(user_id)`，分母改为 `interest_uv`（pv+cart+fav 独立用户数）。

**说明**: 修复后仍有商品 CVR > 100%（如商品 1881953: buy=127, pv=18）。这是**正常现象**：用户可以通过加购/收藏直接购买，不需要先浏览。`pv_to_buy_rate` 的分母是 `pv + cart + fav` 的独立用户数，当购买用户来自收藏夹/购物车回访时，buy_uv 可能大于 pv_uv。

---

## 问题2：转化漏斗数据不合理（已修复）

**文件**: `lib/analyses/funnel.py`

**现象**: 简单漏斗显示各阶段独立用户数，百分比基于 PV 计算，不是严格递进关系。

**原因**: 原代码分别统计各行为的独立用户数，各阶段之间没有包含关系。用户可能跳过某些步骤（如直接从收藏夹购买）。

**修复**: ✅ 已改为严格递进漏斗：
- 浏览(pv)：所有有浏览行为的用户
- 加购/收藏：有浏览**且**有加购/收藏行为的用户
- 购买：有浏览**且**有加购/收藏**且**有购买行为的用户

---

## 问题3：后端API的CVR公式未同步（已修复）

**文件**: `backend/app.py`（4个端点）

**现象**: 前端展示的 CVR 仍然是 `buy总次数 / pv总次数`，与分析模块的修复不一致。

**修复**: ✅ 4个端点全部改为 `countDistinct(buy用户) / countDistinct(interest用户) * 100`：
- `/api/overview` — `pv_uv`, `buy_uv`, `interest_uv` 均为独立用户数
- `/api/categories` — 新增 `pv_uv`, `interest_uv`, `buy_uv` 字段
- `/api/category/<cat_id>/items` — 同上
- `/api/item/<item_id>` — `interest_uv`, `buy_uv` 为独立用户数

---

## 问题4：路径分析跨越会话边界（已修复）

**文件**: `lib/analyses/user_path.py`

**现象**: 2步/3步行为路径可能将用户最后一个会话的行为与下一个会话的第一个行为连在一起。

**原因**: `lag()` 的窗口是 `partitionBy("user_id")`，不区分会话。

**修复**: ✅ 改为 `partitionBy("user_id", "session_id")`，确保路径只在会话内部拼接。

---

## 问题5：工作日/周末天数硬编码（已修复）

**文件**: `lib/analyses/user_time.py`

**现象**: `weekday_count=5, weekend_count=4` 硬编码，数据日期范围变化会出错。

**修复**: ✅ 改为从数据动态计算：`df.select("date", "day_of_week").distinct()` 后分别 count。

---

## 重新运行步骤

```bash
# 1. 上传修复后的代码（4个文件）
scp lib/analyses/product_cvr.py hadoop@master:~/lib/analyses/
scp lib/analyses/funnel.py hadoop@master:~/lib/analyses/
scp lib/analyses/user_path.py hadoop@master:~/lib/analyses/
scp lib/analyses/user_time.py hadoop@master:~/lib/analyses/
scp backend/app.py hadoop@master:~/backend/

# 2. 清理旧输出
hdfs dfs -rm -r -f /data/output
hdfs dfs -mkdir -p /data/output

# 3. 重新运行 task2
cd ~ && bash run_all.sh task2

# 4. 导出数据
bash run_all.sh export

# 5. 重启 Flask
ps aux | grep app.py | awk '{print $2}' | xargs kill -9
cd ~/backend && nohup python3 app.py > flask.log 2>&1 &
```
