"""Static reference data for the synthetic generator.

Everything here is fictional. Sports, leagues, teams and markets are made up
but internally consistent: every team belongs to a league, every league to a
sport, and every market references a real match + outcome set.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Sport:
    code: str
    name: str


@dataclass(frozen=True)
class League:
    code: str
    name: str
    sport_code: str


@dataclass(frozen=True)
class Team:
    team_id: str
    name: str
    league_code: str


@dataclass(frozen=True)
class MarketType:
    code: str
    name: str
    outcomes: tuple[str, ...]


SPORTS: tuple[Sport, ...] = (
    Sport("SOCCER", "Association Football"),
    Sport("BASKETBALL", "Basketball"),
    Sport("AMERICAN_FOOTBALL", "American Football"),
    Sport("TENNIS", "Tennis"),
    Sport("CRICKET", "Cricket"),
    Sport("ESPORTS", "Esports"),
)

LEAGUES: tuple[League, ...] = (
    League("APL", "Apex Premier League", "SOCCER"),
    League("CTL", "Continental Trophy League", "SOCCER"),
    League("HOOPS", "Hoops National League", "BASKETBALL"),
    League("GRIDIRON", "Gridiron Pro Series", "AMERICAN_FOOTBALL"),
    League("COURT", "Court Masters Tour", "TENNIS"),
    League("WILLOW", "Willow Cricket Premiership", "CRICKET"),
    League("NEXUS", "Nexus Esports Championship", "ESPORTS"),
)

_TEAM_NAMES: dict[str, tuple[str, ...]] = {
    "APL": (
        "Northbridge United",
        "Riverton FC",
        "Ashford City",
        "Kingsmoor Rovers",
        "Westport Athletic",
        "Highgate Wanderers",
        "Brookvale Town",
        "Oldcastle FC",
    ),
    "CTL": (
        "Porto Verde",
        "Alpeca CF",
        "Vallemar Sur",
        "Real Cordillera",
        "Estrella Norte",
        "Sporting Bahia",
    ),
    "HOOPS": (
        "Metro Titans",
        "Coastal Spurs",
        "Summit Kings",
        "Harbor Nets",
        "Prairie Lakers",
        "Ironwood Heat",
    ),
    "GRIDIRON": (
        "Steel City Sentinels",
        "Bay Area Breakers",
        "Capital Vanguard",
        "Desert Rattlers",
        "Lakeshore Lightning",
        "Mountain Miners",
    ),
    "COURT": (
        "A. Petrov",
        "L. Nakamura",
        "M. Okafor",
        "S. Delacroix",
        "R. Fernandez",
        "J. Van Dijk",
    ),
    "WILLOW": (
        "Mumbai Monarchs",
        "Colombo Cobras",
        "Karachi Kites",
        "Lahore Lions",
        "Dhaka Dynamos",
    ),
    "NEXUS": ("Voidwalkers", "Neon Syndicate", "Pixel Reapers", "Quantum Foxes", "Byte Bandits"),
}

MARKET_TYPES: dict[str, tuple[MarketType, ...]] = {
    "SOCCER": (
        MarketType("MATCH_RESULT", "Match Result (1X2)", ("HOME", "DRAW", "AWAY")),
        MarketType("OVER_UNDER_2_5", "Total Goals Over/Under 2.5", ("OVER", "UNDER")),
        MarketType("BOTH_TEAMS_SCORE", "Both Teams To Score", ("YES", "NO")),
        MarketType("FIRST_HALF_RESULT", "First Half Result", ("HOME", "DRAW", "AWAY")),
    ),
    "BASKETBALL": (
        MarketType("MONEYLINE", "Moneyline", ("HOME", "AWAY")),
        MarketType("POINTS_SPREAD", "Points Spread", ("HOME", "AWAY")),
        MarketType("TOTAL_POINTS", "Total Points Over/Under", ("OVER", "UNDER")),
    ),
    "AMERICAN_FOOTBALL": (
        MarketType("MONEYLINE", "Moneyline", ("HOME", "AWAY")),
        MarketType("POINTS_SPREAD", "Points Spread", ("HOME", "AWAY")),
        MarketType("TOTAL_POINTS", "Total Points Over/Under", ("OVER", "UNDER")),
    ),
    "TENNIS": (
        MarketType("MATCH_WINNER", "Match Winner", ("HOME", "AWAY")),
        MarketType("SET_BETTING", "Set Betting 2-0/2-1", ("2-0", "2-1", "0-2", "1-2")),
    ),
    "CRICKET": (
        MarketType("MATCH_WINNER", "Match Winner", ("HOME", "AWAY")),
        MarketType("TOP_BATTER", "Top Batsman", ("BATTER_A", "BATTER_B", "BATTER_C")),
    ),
    "ESPORTS": (
        MarketType("MATCH_WINNER", "Series Winner", ("HOME", "AWAY")),
        MarketType("MAP_HANDICAP", "Map Handicap", ("HOME", "AWAY")),
    ),
}

CASINO_GAME_TYPES: tuple[str, ...] = (
    "SLOTS",
    "ROULETTE",
    "BLACKJACK",
    "BACCARAT",
    "POKER",
    "CRASH",
)


@dataclass(frozen=True)
class CurrencyInfo:
    code: str
    symbol: str
    exponent: int = 2


CURRENCIES: tuple[CurrencyInfo, ...] = (
    CurrencyInfo("USD", "$"),
    CurrencyInfo("EUR", "€"),
    CurrencyInfo("GBP", "£"),
    CurrencyInfo("BRL", "R$"),
)

EVENT_SOURCE_SYSTEMS: tuple[str, ...] = (
    "sportsbook-core",
    "casino-engine",
    "payments-gateway",
    "risk-engine",
    "odds-feed-bridge",
    "account-service",
)

CHANNELS: tuple[str, ...] = (
    "mobile_web",
    "mobile_ios",
    "mobile_android",
    "desktop_web",
    "retail_terminal",
)

DEVICES: tuple[str, ...] = ("web", "ios", "android")


@dataclass(frozen=True)
class TeamPair:
    home: Team
    away: Team


def teams_by_league(league_code: str) -> list[Team]:
    return [
        Team(f"team_{league_code.lower()}_{i}", name, league_code)
        for i, name in enumerate(_TEAM_NAMES[league_code], start=1)
    ]


@dataclass
class ReferenceData:
    """Materialized reference data with lookup helpers."""

    sports: dict[str, Sport] = field(default_factory=dict)
    leagues: dict[str, League] = field(default_factory=dict)
    teams: dict[str, Team] = field(default_factory=dict)
    markets: dict[str, MarketType] = field(default_factory=dict)
    currencies: dict[str, CurrencyInfo] = field(default_factory=dict)

    @classmethod
    def build(cls) -> ReferenceData:
        rd = cls()
        rd.sports = {s.code: s for s in SPORTS}
        rd.leagues = {lg.code: lg for lg in LEAGUES}
        for lg in LEAGUES:
            for team in teams_by_league(lg.code):
                rd.teams[team.team_id] = team
        for mts in MARKET_TYPES.values():
            for mt in mts:
                rd.markets[mt.code] = mt
        rd.currencies = {c.code: c for c in CURRENCIES}
        return rd

    def teams_for_league(self, league_code: str) -> list[Team]:
        return [t for t in self.teams.values() if t.league_code == league_code]

    def market_types_for_sport(self, sport_code: str) -> tuple[MarketType, ...]:
        return MARKET_TYPES.get(sport_code, ())
