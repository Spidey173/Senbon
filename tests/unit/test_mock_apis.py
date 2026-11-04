"""
Unit Tests: Mock REST APIs
==========================
Validates FastAPI endpoints and response schemas for streaming feeds.
"""

import pytest
from fastapi.testclient import TestClient
from scripts.mock_api_server import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert "timestamp" in data


def test_daily_sales_endpoint():
    response = client.get("/api/v1/sales/daily?limit=10")
    assert response.status_code == 200
    sales = response.json()
    assert len(sales) == 10
    first = sales[0]
    assert "api_sale_id" in first
    assert "gross_amount" in first
    assert "net_amount" in first
    assert first["currency"] == "USD"


def test_currency_rates_endpoint():
    response = client.get("/api/v1/currency/rates")
    assert response.status_code == 200
    rates = response.json()
    assert len(rates) >= 4
    currencies = [r["target_currency"] for r in rates]
    assert "USD" in currencies
    assert "EUR" in currencies


def test_store_weather_endpoint():
    response = client.get("/api/v1/weather/stores")
    assert response.status_code == 200
    weather = response.json()
    assert len(weather) == 10
    assert weather[0]["weather_condition"] in ["SUNNY", "RAIN", "SNOW", "CLOUDY", "THUNDERSTORM"]

