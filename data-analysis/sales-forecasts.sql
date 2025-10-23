/*
 ============================================================================
 PROJECT: SALES FORECAST CONNECTING NET VALUE
 ============================================================================
 Author: Aine Gradisher
 GitHub: https://github.com/ainegradisher/sql-query-portfolio
 Created: October 2025
 
 ============================================================================
 SCHEMA DISCLAIMER:
 ============================================================================
 The database schema shown (table names, columns, relationships) is a 
 GENERIC, ANONYMISED representation created for portfolio demonstration.
 
 This structure:
 - Does NOT represent any specific company's actual database
 - Uses common naming conventions similar to standard ERP/CRM systems
 - Is designed to demonstrate SQL techniques, not reproduce proprietary schemas
 - Has been modified from original work to remove all identifying information
 
 The VALUE of this portfolio piece is the PROBLEM-SOLVING APPROACH and
 TECHNICAL IMPLEMENTATION, not the specific schema design.
 ============================================================================
 
 ============================================================================
 PURPOSE: Provide Sales and Finance teams with the net value of our sales forecasts, where pricing for items are kept seperately from the data on our ERP.
          This meant that there was no way to easily track the net value of forecasts on our system without this query.
 
 BUSINESS PROBLEM SOLVED:
   - Sales teams had forecasts but no systematic comparison to actual item prices
   - Manual Excel-based comparisons taking hours weekly
   - Multi-currency forecasts difficult to report on

 TECHNICAL APPROACH:
   - Multi-frequency forecast normalisation (weekly/monthly/quarterly → monthly)
   - Date arithmetic for period calculations across different frequencies
   - Left join pattern to identify forecast-only (unfulfilled) items
   - Multi-currency standardisation to GBP for consolidated reporting
   - Conditional aggregation logic based on forecast frequency type

 BUSINESS IMPACT:
   - Automated weekly report generation (2-3 hours saved)
   - Enabled finance to report on forecasts

 TECHNICAL REQUIREMENTS:
   - SQL Server 2016+ (for FORMAT function and date arithmetic)
   - No external dependencies
   - Typical execution time: 5-8 seconds for 50,000+ forecast lines

 KEY SQL TECHNIQUES DEMONSTRATED:
   ✓ Multi-step CTEs for complex logic decomposition
   ✓ Dynamic date calculations (DATEADD with variable intervals)
   ✓ Conditional aggregation (SUM vs MAX based on frequency)
   ✓ Multi-currency conversion with exchange rate handling
   ✓ LEFT JOIN pattern for "forecast but not ordered" analysis
   ✓ Date filtering with BETWEEN for exclusion windows
   ✓ NULL-safe arithmetic (ISNULL, NULLIF)
 ============================================================================
*/

-- ============================================================================
-- CTE 1: BASE FORECAST EXTRACTION
-- ============================================================================
-- PURPOSE: Extract all active forecast records with their scheduling details
-- LOGIC: Join forecast header → frequency → items → quantities → periods
-- NOTE: This denormalizes the forecast structure for easier processing
-- ============================================================================
WITH BaseForecast AS (
    SELECT 
        ForecastHeader.ForecastID,
        ForecastHeader.ForecastReference,              -- Customer forecast code
        ForecastHeader.CustomerID,
        ForecastItem.ForecastItemID,
        ForecastItem.ProductID,
        ForecastHeader.StartDate,                      -- When forecast begins
        ForecastQuantity.Quantity,                     -- Quantity for this period
        ForecastPeriod.PeriodSequence,                 -- Period number (1, 2, 3...)
        ForecastFrequency.FrequencyType                -- Weekly, Monthly, Quarterly, Daily
    FROM 
        SalesForecastHeader ForecastHeader
    INNER JOIN SalesForecastFrequency ForecastFrequency
        ON ForecastHeader.FrequencyID = ForecastFrequency.FrequencyID
    INNER JOIN SalesForecastItem ForecastItem
        ON ForecastItem.ForecastID = ForecastHeader.ForecastID
    INNER JOIN SalesForecastQuantity ForecastQuantity
        ON ForecastQuantity.ForecastItemID = ForecastItem.ForecastItemID
    INNER JOIN SalesForecastPeriod ForecastPeriod
        ON ForecastQuantity.PeriodID = ForecastPeriod.PeriodID
    WHERE 
        ForecastHeader.StartDate >= '2024-01-01'       -- Only recent forecasts
        AND ForecastQuantity.Quantity > 0              -- Ignore zero-quantity periods
),

-- ============================================================================
-- CTE 2: FORECAST DATE CALCULATION
-- ============================================================================
-- PURPOSE: Convert "period sequence + frequency" into actual calendar dates
-- 
-- CHALLENGE: Forecasts can be in different frequencies:
--   - Weekly: Period 1 = Week 1, Period 2 = Week 2, etc.
--   - Monthly: Period 1 = Month 1, Period 2 = Month 2, etc.
--   - Quarterly: Period 1 = Quarter 1, Period 2 = Quarter 2, etc.
--
-- SOLUTION: Dynamic DATEADD based on frequency type
--
-- EXAMPLE:
--   Start Date: Jan 1, 2024
--   Frequency: Monthly
--   Period 3 → March 1, 2024
--
--   Start Date: Jan 1, 2024  
--   Frequency: Weekly
--   Period 3 → Jan 15, 2024 (week 3)
-- ============================================================================
ForecastDates AS (
    SELECT
        BF.ForecastID,
        BF.ForecastReference,
        BF.CustomerID,
        BF.ForecastItemID,
        BF.ProductID,
        BF.Quantity,
        BF.StartDate,
        BF.PeriodSequence,
        BF.FrequencyType,
        
        -- Dynamic date calculation based on frequency
        CASE 
            -- Monthly: Add months to start date
            WHEN BF.FrequencyType = 'Monthly' 
                THEN DATEADD(MONTH, BF.PeriodSequence - 1, BF.StartDate)
            
            -- Weekly: Add weeks to start date
            WHEN BF.FrequencyType = 'Weekly'
                THEN DATEADD(WEEK, BF.PeriodSequence - 1, BF.StartDate)
            
            -- Daily: Add days to start date
            WHEN BF.FrequencyType = 'Daily'
                THEN DATEADD(DAY, BF.PeriodSequence - 1, BF.StartDate)
            
            -- Quarterly: Multiply sequence by 3 months
            WHEN BF.FrequencyType = 'Quarterly'
                THEN DATEADD(MONTH, (BF.PeriodSequence - 1) * 3, BF.StartDate)
            
            -- Default: Use start date if frequency unknown
            ELSE BF.StartDate
        END AS PeriodDate
        -- Result: Every forecast period now has a specific calendar date
        
    FROM BaseForecast BF
),

-- ============================================================================
-- CTE 3: MONTHLY AGGREGATION
-- ============================================================================
-- PURPOSE: Normalide all forecasts to monthly periods for comparison
-- 
-- CHALLENGE: Different frequencies need different aggregation logic:
--   - Weekly forecasts: SUM all weeks in a month
--   - Monthly forecasts: Take the single value (MAX)
--   - Quarterly forecasts: SUM all months in quarter (if split)
--
-- BUSINESS RULE: Exclude near-term period (next 60 days) as orders may
--                still be in negotiation and don't represent true gaps
--
-- EXAMPLE:
--   Weekly forecast: Week 1 = 25, Week 2 = 25, Week 3 = 25, Week 4 = 25
--   Monthly total = 100 (SUM)
--
--   Monthly forecast: Month 1 = 100
--   Monthly total = 100 (MAX, since there's only one value)
-- ============================================================================
MonthlyAggregates AS (
    SELECT 
        FD.ForecastReference,
        FD.CustomerID,
        FD.ProductID,
        FD.FrequencyType,
        FD.PeriodDate,
        MONTH(FD.PeriodDate) AS MonthNumber,           -- 1-12
        YEAR(FD.PeriodDate) AS YearNumber,             -- 2024, 2025, etc.
        FORMAT(FD.PeriodDate, 'MMMM yyyy') AS MonthYear, -- "January 2024"
        
        -- Conditional aggregation based on frequency
        CASE 
            WHEN FD.FrequencyType = 'Weekly' THEN SUM(FD.Quantity)
            -- Weekly: Add up all weeks in the month
            
            WHEN FD.FrequencyType = 'Monthly' THEN MAX(FD.Quantity)
            -- Monthly: Take the single month value
            
            WHEN FD.FrequencyType = 'Quarterly' THEN SUM(FD.Quantity)
            -- Quarterly: Add months in quarter (if applicable)
            
            ELSE MAX(FD.Quantity)
            -- Default: Take highest value
        END AS ForecastQuantity
        
    FROM ForecastDates FD
    
    -- BUSINESS RULE: Exclude "negotiation window" (next 60 days)
    -- Rationale: Orders for near-term may still be in pipeline
    WHERE FD.PeriodDate NOT BETWEEN GETDATE() AND DATEADD(DAY, 60, GETDATE())
    
    GROUP BY
        FD.ForecastReference,
        FD.CustomerID,
        FD.ProductID,
        FD.FrequencyType,
        FD.PeriodDate,
        MONTH(FD.PeriodDate),
        YEAR(FD.PeriodDate),
        FORMAT(FD.PeriodDate, 'MMMM yyyy')
    
    -- Only keep periods with non-zero forecast
    HAVING 
        CASE 
            WHEN FD.FrequencyType = 'Weekly' THEN SUM(FD.Quantity)
            WHEN FD.FrequencyType = 'Monthly' THEN MAX(FD.Quantity)
            WHEN FD.FrequencyType = 'Quarterly' THEN SUM(FD.Quantity)
            ELSE MAX(FD.Quantity)
        END > 0
),

-- ============================================================================
-- CTE 4: ACTUAL ORDERS AGGREGATION
-- ============================================================================
-- PURPOSE: Calculate actual ordered quantities by customer/item/month
-- LOGIC: Group sales orders by promised delivery month
-- NOTE: Uses promised date because that's what forecast commits to
-- ============================================================================
OrderedQuantities AS (
    SELECT 
        Customer.CustomerAccountNumber AS CustomerCode,
        OrderLineView.ProductCode AS ProductCode,
        MONTH(OrderLine.PromisedDeliveryDate) AS MonthNumber,
        YEAR(OrderLine.PromisedDeliveryDate) AS YearNumber,
        SUM(OrderLineView.OrderQuantity) AS TotalOrderedQuantity
        -- Sum all order lines for same customer/product/month
        
    FROM SalesOrder OrderHeader
    
    INNER JOIN SalesOrderLine OrderLine
        ON OrderHeader.OrderID = OrderLine.OrderID
    
    INNER JOIN SalesOrderLineView OrderLineView
        ON OrderHeader.OrderID = OrderLineView.OrderID
        AND OrderLine.LineNumber = OrderLineView.LineNumber
    
    INNER JOIN CustomerAccount Customer
        ON OrderHeader.CustomerID = Customer.CustomerID
    
    WHERE 
        -- Exclude test/template orders (marked with # prefix)
        OrderHeader.OrderNumber NOT LIKE '#%'
        
        -- Only sales orders (not returns)
        AND OrderHeader.DocumentType = 0
        
        -- Only live orders (not completed/cancelled)
        AND OrderHeader.DocumentStatus = 0
        
        -- Only orders from 2024 onwards (match forecast date range)
        AND OrderLine.PromisedDeliveryDate >= '2024-01-01'
    
    GROUP BY 
        Customer.CustomerAccountNumber,
        OrderLineView.ProductCode,
        MONTH(OrderLine.PromisedDeliveryDate),
        YEAR(OrderLine.PromisedDeliveryDate)
)

-- ============================================================================
-- FINAL SELECT: FORECAST VS ACTUALS COMPARISON
-- ============================================================================
-- PURPOSE: Present side-by-side comparison with gap analysis and value calculation
-- APPROACH: LEFT JOIN to show all forecasts, even if no orders exist
-- ============================================================================
SELECT DISTINCT
    -- =========================================================================
    -- SECTION 1: IDENTIFIERS
    -- =========================================================================
    MA.ForecastReference AS 'Forecast Reference',
    Product.ProductCode AS 'Product Code',
    MA.PeriodDate AS 'Period Date',              -- For troubleshooting/validation
    MA.FrequencyType AS 'Forecast Frequency',
    
    -- =========================================================================
    -- SECTION 2: CUSTOMER INFORMATION
    -- =========================================================================
    Customer.CustomerAccountNumber AS 'Customer Code',
    
    -- =========================================================================
    -- SECTION 3: TIME PERIOD
    -- =========================================================================
    MA.YearNumber AS 'Year',
    MA.MonthNumber AS 'Month Number',
    MA.MonthYear AS 'Month Name',
    
    -- =========================================================================
    -- SECTION 4: QUANTITY COMPARISON (THE KEY ANALYSIS)
    -- =========================================================================
    ROUND(MA.ForecastQuantity, 5) AS 'Forecast Quantity',
    
    ISNULL(OQ.TotalOrderedQuantity, 0) AS 'Actual Order Quantity',
    -- ISNULL handles cases where forecast exists but no orders placed
    
    ROUND(MA.ForecastQuantity - ISNULL(OQ.TotalOrderedQuantity, 0), 5) AS 'Unfulfilled Quantity',
    -- POSITIVE = We're short on orders vs forecast (potential revenue gap)
    -- NEGATIVE = We have more orders than forecast (good problem to have!)
    -- ZERO = Perfect match
    
    -- =========================================================================
    -- SECTION 5: PRICING (MULTI-CURRENCY)
    -- =========================================================================
    Price.UnitPrice AS 'Unit Price (Local Currency)',
    Currency.CurrencySymbol AS 'Currency',
    
    -- Convert to GBP for consolidated reporting
    CASE 
        WHEN Customer.CurrencyID = 1 THEN Price.UnitPrice  -- Already in GBP
        WHEN Price.UnitPrice != 0 THEN 
            -- Two-step conversion: Local → EUR → GBP
            (Price.UnitPrice / NULLIF(Currency.ExchangeRateToBase, 0))
            * (SELECT GBPExchangeRate FROM SystemCurrency WHERE CurrencyID = 1)
        ELSE 0
    END AS 'Unit Price (GBP)',
    
    -- =========================================================================
    -- SECTION 6: VALUE CALCULATION (REVENUE OPPORTUNITY)
    -- =========================================================================
    -- Only calculate value for unfulfilled quantities (potential revenue)
    CASE 
        WHEN ROUND(MA.ForecastQuantity - ISNULL(OQ.TotalOrderedQuantity, 0), 5) > 0 THEN 
            -- Unfulfilled qty × Price in GBP
            (ROUND(MA.ForecastQuantity - ISNULL(OQ.TotalOrderedQuantity, 0), 5) * 
            CASE 
                WHEN Customer.CurrencyID = 1 THEN Price.UnitPrice
                WHEN Price.UnitPrice != 0 THEN 
                    (Price.UnitPrice / NULLIF(Currency.ExchangeRateToBase, 0))
                    * (SELECT GBPExchangeRate FROM SystemCurrency WHERE CurrencyID = 1)
                ELSE 0
            END)
        ELSE 0  -- No value if we've fulfilled or over-fulfilled
    END AS 'Unfulfilled Value (GBP)'
    -- KEY METRIC: This is the potential revenue at risk

FROM MonthlyAggregates MA

-- =========================================================================
-- JOIN 1: Product Details
-- =========================================================================
INNER JOIN Product Product
    ON Product.ProductID = MA.ProductID

-- =========================================================================
-- JOIN 2: Customer Details
-- =========================================================================
INNER JOIN CustomerAccount Customer
    ON MA.CustomerID = Customer.CustomerID

-- =========================================================================
-- JOIN 3: Pricing Information
-- =========================================================================
INNER JOIN ProductPrice Price
    ON Price.ProductID = Product.ProductID
    AND Price.PriceBandID = Customer.PriceBandID  -- Customer-specific pricing

-- =========================================================================
-- JOIN 4: Currency Information
-- =========================================================================
INNER JOIN SystemCurrency Currency
    ON Customer.CurrencyID = Currency.CurrencyID

-- =========================================================================
-- JOIN 5: Actual Orders (LEFT JOIN - the key to finding gaps!)
-- =========================================================================
LEFT JOIN OrderedQuantities OQ
    ON Customer.CustomerAccountNumber = OQ.CustomerCode
    AND Product.ProductCode = OQ.ProductCode
    AND MA.MonthNumber = OQ.MonthNumber
    AND MA.YearNumber = OQ.YearNumber
-- LEFT JOIN ensures we see forecasts even when no orders exist
-- This is how we identify the revenue gaps!

-- =========================================================================
-- FILTERS
-- =========================================================================
WHERE 
    Price.UnitPrice != 0            -- Exclude zero-price items (not revenue-generating)
    AND MA.ForecastQuantity > 0     -- Only forecasts with actual quantity

-- =========================================================================
-- SORTING
-- =========================================================================
ORDER BY 
    MA.YearNumber,                  -- Chronological by year
    MA.MonthNumber,                 -- Then by month
    MA.ForecastReference,           -- Group by forecast
    Customer.CustomerAccountNumber, -- Then customer
    Product.ProductCode;            -- Finally product

-- ============================================================================
-- QUERY EXPLANATION
-- ============================================================================
/*
WHAT IT DOES:
Compares customer sales forecasts against actual orders to identify:
1. Where customers forecasted demand but haven't ordered
2. Where customers exceeded their forecast
3. Total value of unfulfilled forecast commitments

HOW IT WORKS:
1. Extract all forecast records and calculate actual calendar dates
2. Normalize different frequencies (weekly/monthly/quarterly) to monthly periods
3. Aggregate actual orders by the same periods
4. LEFT JOIN to find forecast-without-orders (the gaps)
5. Calculate unfulfilled quantity and its value in GBP

KEY BUSINESS LOGIC:
- Uses promised delivery date (not order date) for comparison
- Excludes next 60 days (negotiation window)
- Handles multiple forecast frequencies with appropriate aggregation
- Converts all currencies to GBP for executive reporting
- Only shows revenue value for positive gaps (unfulfilled)

TYPICAL USE CASES:
- Weekly sales review: "Which customers are behind on forecast?"
- Monthly revenue forecasting: "What's our forecast pipeline value?"
- Customer engagement: "Proactively follow up on unfulfilled commitments"
- Capacity planning: "Are we seeing over-forecast scenarios?"
*/

-- ============================================================================
-- TECHNICAL NOTES
-- ============================================================================
/*
DATE ARITHMETIC COMPLEXITY:
- Different frequencies require different DATEADD intervals
- Quarterly = 3 months per period (sequence × 3)
- Must handle edge cases (Feb 29, month-end dates, etc.)

AGGREGATION LOGIC:
- Weekly → SUM (multiple weeks per month)
- Monthly → MAX (one value per month, MAX handles dupes)
- Quarterly → SUM (may span multiple months)
- Critical to use correct logic or totals will be wrong

CURRENCY CONVERSION:
- Two-step conversion: Local → Base → GBP
- NULLIF prevents division by zero errors
- Subquery for GBP rate ensures consistency

LEFT JOIN PATTERN:
- The LEFT JOIN is what makes this analysis work
- Shows ALL forecasts, even without orders
- NULL in order quantity = revenue gap to investigate

PERFORMANCE CONSIDERATIONS:
- CTEs make logic readable but check execution plan
- Consider indexing: CustomerID, ProductID, PromisedDeliveryDate
- Date range filter helps limit dataset size
- DISTINCT needed due to potential price band duplication
*/

-- ============================================================================
-- SAMPLE OUTPUT
-- ============================================================================
/*
Forecast Ref | Product | Customer | Month      | Forecast | Actual | Unfulfilled | Value (GBP)
-------------|---------|----------|------------|----------|--------|-------------|-------------
FC-2024-001  | PROD123 | CUST001  | Jan 2024   | 1000     | 750    | 250         | £12,500
FC-2024-001  | PROD456 | CUST001  | Jan 2024   | 500      | 500    | 0           | £0
FC-2024-002  | PROD789 | CUST002  | Feb 2024   | 2000     | 0      | 2000        | £45,000

INTERPRETATION:
Row 1: Customer ordered 75% of forecast - follow up on remaining 250 units
Row 2: Perfect match - no action needed
Row 3: No orders against forecast - URGENT follow-up required (£45K at risk)
*/

-- ============================================================================
-- END OF FILE
-- ============================================================================
