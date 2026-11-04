"""
Senbon — Retail Intelligence Mock REST API Service
===================================================
Simulates enterprise external endpoints (daily live sales stream, currency exchange rates,
store weather data, and marketing campaign attribution) using FastAPI.
"""

import os
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel

app = FastAPI(
    title="Senbon — Retail Intelligence External APIs",
    description="Microservices simulating external data feeds for Senbon retail lakehouse ingestion.",
    version="1.0.0",
)


class DailySalesRecord(BaseModel):
    api_sale_id: str
    store_id: str
    channel: str  # ONLINE, IN_STORE, MOBILE_APP
    sale_timestamp: str
    item_count: int
    gross_amount: float
    discount_amount: float
    net_amount: float
    currency: str
    payment_type: str


class CurrencyRate(BaseModel):
    base_currency: str
    target_currency: str
    exchange_rate: float
    effective_date: str


class StoreWeatherRecord(BaseModel):
    store_id: str
    city: str
    state: str
    weather_condition: str  # SUNNY, RAIN, SNOW, CLOUDY, STORM
    temperature_celsius: float
    precipitation_mm: float
    recorded_at: str


class MarketingCampaignRecord(BaseModel):
    campaign_id: str
    campaign_name: str
    platform: str  # GOOGLE_ADS, META, TIKTOK, EMAIL
    start_date: str
    end_date: str
    budget_usd: float
    spend_usd: float
    impressions: int
    clicks: int
    conversions: int


@app.get("/health", tags=["Health"])
def health_check() -> Dict[str, str]:
    return {"status": "HEALTHY", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/sales/daily", response_model=List[DailySalesRecord], tags=["Sales"])
def get_daily_sales(
    date: Optional[str] = Query(None, description="ISO Date string YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=500),
) -> List[DailySalesRecord]:
    """Returns streaming/micro-batch daily retail sales across online & mobile channels."""
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    channels = ["ONLINE", "MOBILE_APP", "IN_STORE", "MARKETPLACE"]
    payment_types = ["CREDIT_CARD", "APPLE_PAY", "PAYPAL", "KLARNA"]

    records = []
    for i in range(1, limit + 1):
        gross = round(random.uniform(15.0, 450.0), 2)
        discount = round(gross * random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
        net = round(gross - discount, 2)
        store_num = random.randint(1, 10)

        record = DailySalesRecord(
            api_sale_id=f"API_SALE_{target_date.replace('-', '')}_{i:04d}",
            store_id=f"STR_{store_num:03d}",
            channel=random.choice(channels),
            sale_timestamp=f"{target_date}T{random.randint(8, 22):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}Z",
            item_count=random.randint(1, 8),
            gross_amount=gross,
            discount_amount=discount,
            net_amount=net,
            currency="USD",
            payment_type=random.choice(payment_types),
        )
        records.append(record)

    return records


@app.get("/api/v1/currency/rates", response_model=List[CurrencyRate], tags=["Finance"])
def get_currency_rates() -> List[CurrencyRate]:
    """Returns forex exchange rates used to normalize multi-currency transactions into USD."""
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rates = [
        {"target_currency": "USD", "rate": 1.0000},
        {"target_currency": "EUR", "rate": 1.0850},
        {"target_currency": "GBP", "rate": 1.2720},
        {"target_currency": "CAD", "rate": 0.7350},
        {"target_currency": "JPY", "rate": 0.0064},
    ]

    return [
        CurrencyRate(
            base_currency="USD",
            target_currency=item["target_currency"],
            exchange_rate=item["rate"],
            effective_date=today_str,
        )
        for item in rates
    ]


@app.get("/api/v1/weather/stores", response_model=List[StoreWeatherRecord], tags=["Operations"])
def get_store_weather() -> List[StoreWeatherRecord]:
    """Returns store-level weather feeds to analyze foot traffic suppression."""
    stores_metadata = [
        {"id": "STR_001", "city": "New York", "state": "NY"},
        {"id": "STR_002", "city": "San Francisco", "state": "CA"},
        {"id": "STR_003", "city": "Chicago", "state": "IL"},
        {"id": "STR_004", "city": "Austin", "state": "TX"},
        {"id": "STR_005", "city": "Seattle", "state": "WA"},
        {"id": "STR_006", "city": "Miami", "state": "FL"},
        {"id": "STR_007", "city": "Denver", "state": "CO"},
        {"id": "STR_008", "city": "Boston", "state": "MA"},
        {"id": "STR_009", "city": "Atlanta", "state": "GA"},
        {"id": "STR_010", "city": "Los Angeles", "state": "CA"},
    ]
    conditions = ["SUNNY", "RAIN", "SNOW", "CLOUDY", "THUNDERSTORM"]
    now_iso = datetime.now(timezone.utc).isoformat()

    results = []
    for store in stores_metadata:
        cond = random.choice(conditions)
        temp = round(random.uniform(-5.0, 32.0), 1)
        precip = round(random.uniform(0.0, 25.0) if cond in ["RAIN", "SNOW", "THUNDERSTORM"] else 0.0, 1)

        results.append(
            StoreWeatherRecord(
                store_id=store["id"],
                city=store["city"],
                state=store["state"],
                weather_condition=cond,
                temperature_celsius=temp,
                precipitation_mm=precip,
                recorded_at=now_iso,
            )
        )
    return results


@app.get("/api/v1/marketing/campaigns", response_model=List[MarketingCampaignRecord], tags=["Marketing"])
def get_marketing_campaigns() -> List[MarketingCampaignRecord]:
    """Returns digital marketing campaign metrics for ad spend ROI calculations."""
    campaigns = [
        ("CMP_SUMMER_2026", "Summer Apparel Blowout", "META", 50000.0, 48200.0, 1200000, 36000, 2400),
        ("CMP_SEARCH_BRAND", "Always-On Brand Search", "GOOGLE_ADS", 80000.0, 79100.0, 2400000, 96000, 6800),
        ("CMP_TIKTOK_VIRAL", "Gen Z TikTok Creator Push", "TIKTOK", 35000.0, 34200.0, 4500000, 180000, 4100),
        ("CMP_LOYALTY_EMAIL", "VIP Loyalty Re-engagement", "EMAIL", 12000.0, 11500.0, 400000, 60000, 5200),
        ("CMP_BACK2SCHOOL", "Autumn Back to Campus", "GOOGLE_ADS", 45000.0, 41200.0, 1500000, 52000, 3100),
    ]

    return [
        MarketingCampaignRecord(
            campaign_id=c[0],
            campaign_name=c[1],
            platform=c[2],
            start_date="2026-06-01",
            end_date="2026-08-31",
            budget_usd=c[3],
            spend_usd=c[4],
            impressions=c[5],
            clicks=c[6],
            conversions=c[7],
        )
        for c in campaigns
    ]


if __name__ == "__main__":
    import uvicorn
    host = os.getenv("MOCK_API_HOST", "0.0.0.0")
    port = int(os.getenv("MOCK_API_PORT", "8000"))
    uvicorn.run("mock_api_server:app", host=host, port=port, reload=False)

