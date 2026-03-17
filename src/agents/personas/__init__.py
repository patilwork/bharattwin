"""
Agent persona registry.
"""

from src.agents.personas.fii_quant import PERSONA as FII_QUANT
from src.agents.personas.retail_momentum import PERSONA as RETAIL_MOMENTUM
from src.agents.personas.dealer_hedging import PERSONA as DEALER_HEDGING
from src.agents.personas.dii_mf import PERSONA as DII_MF
from src.agents.personas.macro import PERSONA as MACRO
from src.agents.personas.sector_rotation import PERSONA as SECTOR_ROTATION
from src.agents.personas.corp_earnings import PERSONA as CORP_EARNINGS
from src.agents.personas.event_news import PERSONA as EVENT_NEWS
from src.agents.personas.operator import PERSONA as OPERATOR
from src.agents.personas.dabba_speculator import PERSONA as DABBA_SPECULATOR

ALL_PERSONAS = [
    FII_QUANT,
    RETAIL_MOMENTUM,
    DEALER_HEDGING,
    DII_MF,
    MACRO,
    SECTOR_ROTATION,
    CORP_EARNINGS,
    EVENT_NEWS,
    OPERATOR,
    DABBA_SPECULATOR,
]

PERSONA_BY_ID = {p.agent_id: p for p in ALL_PERSONAS}
