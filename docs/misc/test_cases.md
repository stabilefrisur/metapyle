separate yaml files:
```yaml
# Bloomberg
- my_name: sp500_close
  source: bloomberg
  symbol: SPX Index
  field: PX_LAST
  description: S&P 500 closing price
  unit: points

- my_name: sp500_volume
  source: bloomberg
  symbol: SPX Index
  field: PX_VOLUME
  description: S&P 500 trading volume

- my_name: us_gdp
  source: bloomberg
  symbol: GDP CUR$ Index
  description: US GDP in current dollars
  unit: USD billions

- my_name: us_cpi_yoy
  source: bloomberg
  symbol: CPI YOY Index
  description: US CPI year-over-year change
  unit: percent
```

```yaml
# Macrobond
- my_name: sp500_mb
  source: macrobond
  symbol: ih:bl:spx index
  description: S&P 500 Index
  unit: points

- my_name: us_gdp_mb
  source: macrobond
  symbol: usnaac0169
  description: US GDP constant prices SA AR
  unit: USD trillions

- my_name: cmbs_bbb
  source: macrobond
  symbol: ih:mb:priv:xsa_spread_cmbs_bbb
  description: CMBS BBB spread
  unit: basis points
```

feature cases: 
- single series with bloomberg: sp500_close
- single series with macrobond: sp500_mb
- multiple fields of same symbol with bloomberg: sp500_close + sp500_volume
- multiple sources same frequency: sp500_close + sp500_mb (daily) / us_gdp + us_gdp_mb (quarterly)
- multiple sources different frequency: sp500_close + us_gdp_mb
- multiple sources aligned frequency: sp500_close + us_gdp_mb (use frequency="B")
- align frequency with bloomberg: sp500_close (daily) + us_cpi_yoy (monthly) / us_gdp (quarterly) + us_cpi_yoy (monthly)
- align frequency with macrobond (client side, use align=True): sp500_mb + us_gdp_mb
- align frequency with macrobond (server side, use unified=True): sp500_mb + us_gdp_mb
- private inhouse series in macrobond: cmbs_bbb
