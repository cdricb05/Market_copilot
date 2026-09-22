#!/usr/bin/env python3
"""
Publish R65 feature sets to the registry via AgentPipeline.
"""
import sys
import os
sys.path.insert(0, 'C:\\Users\\binis\\paper_trader')
os.chdir('C:\\Users\\binis\\paper_trader')

from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.alpha_agent.agents_v2.pipeline import AgentPipeline

mem = M.open_memory()
p = AgentPipeline(mem)

# Feature set 1: fs_r65_01_intl_index_oi_growth
features_01 = [
    {
        "name": "oi_growth_21_63_mean_ratio_held_contract",
        "lag": 2,
        "source": "R38 native dated-contract layer, exchange daily feed",
        "availability_instant": "Session t-1 (next business day after session t closes), revised-safe at session t-2"
    }
]

try:
    p.publish_features(
        agent="feature-library-agent",
        feature_set_id="fs_r65_01_intl_index_oi_growth",
        universe_id="r65_intl_index_oi_covered",
        features=features_01,
        leakage_check="PASS"
    )
    print("SUCCESS: fs_r65_01_intl_index_oi_growth published")
except Exception as e:
    print(f"FAILED: fs_r65_01_intl_index_oi_growth - {str(e)}")
    import traceback
    traceback.print_exc()

# Feature set 2: fs_r65_02_metals_currency_leadlag_h3
features_02 = [
    {
        "name": "metals_fx_leadlag_h3_sparse_decile_trigger",
        "lag": 0,
        "source": "R38 native dated-contract layer, held contract, daily prices",
        "availability_instant": "Session t (end of day when both metals and FX markets have closed)"
    }
]

try:
    p.publish_features(
        agent="feature-library-agent",
        feature_set_id="fs_r65_02_metals_currency_leadlag_h3",
        universe_id="r65_xa_metals_currency_leadlag",
        features=features_02,
        leakage_check="PASS"
    )
    print("SUCCESS: fs_r65_02_metals_currency_leadlag_h3 published")
except Exception as e:
    print(f"FAILED: fs_r65_02_metals_currency_leadlag_h3 - {str(e)}")
    import traceback
    traceback.print_exc()

# Feature set 3: fs_r65_03_commodity_oi_growth
features_03 = [
    {
        "name": "oi_growth_21_63_mean_ratio_held_contract_group_demeaned",
        "lag": 2,
        "source": "R38 native dated-contract layer, exchange daily feed",
        "availability_instant": "Session t-1 (next business day after session t closes), revised-safe at session t-2"
    }
]

try:
    p.publish_features(
        agent="feature-library-agent",
        feature_set_id="fs_r65_03_commodity_oi_growth",
        universe_id="r65_commodity_oi_covered",
        features=features_03,
        leakage_check="PASS"
    )
    print("SUCCESS: fs_r65_03_commodity_oi_growth published")
except Exception as e:
    print(f"FAILED: fs_r65_03_commodity_oi_growth - {str(e)}")
    import traceback
    traceback.print_exc()

print("\nFeature publication complete.")
