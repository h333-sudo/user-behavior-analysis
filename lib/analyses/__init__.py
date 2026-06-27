# analyses 包：分析模块集合
# 通过 discover_analyses() 自动发现所有带 MODULE_ID 的模块

import importlib
import pkgutil
import os


def discover_analyses(config=None):
    """
    自动扫描 lib/analyses/ 下所有模块，读取元数据，应用启用/关闭逻辑。

    优先级：CLI --only/--skip/--enable > config.yaml disabled > MODULE_ENABLED

    参数:
        config: 配置字典（可选），用于读取 analysis.disabled 列表

    返回:
        list[dict]: 模块列表，每个元素包含 id/name/group/outputs/enabled/run
    """
    disabled = set()
    if config and "analysis" in config:
        disabled = set(config["analysis"].get("disabled", []))

    analyses = []
    package_dir = os.path.dirname(__file__)

    for _, module_name, _ in pkgutil.iter_modules([package_dir]):
        if module_name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f".{module_name}", package="lib.analyses")
        except Exception as e:
            print(f"[警告] 无法加载模块 {module_name}: {e}")
            continue

        if not hasattr(mod, "MODULE_ID"):
            continue

        module_id = mod.MODULE_ID
        default_enabled = getattr(mod, "MODULE_ENABLED", True)
        is_enabled = default_enabled and module_id not in disabled

        analyses.append({
            "id": module_id,
            "name": mod.MODULE_NAME,
            "group": getattr(mod, "MODULE_GROUP", "other"),
            "outputs": getattr(mod, "OUTPUT_FILES", []),
            "enabled": is_enabled,
            "enabled_reason": _get_enabled_reason(default_enabled, module_id, disabled),
            "run": mod.run,
        })

    return sorted(analyses, key=lambda x: x["id"])


def _get_enabled_reason(default_enabled, module_id, disabled):
    """返回模块启用/关闭的原因"""
    if not default_enabled:
        return "MODULE_ENABLED=False"
    if module_id in disabled:
        return "config disabled"
    return ""
