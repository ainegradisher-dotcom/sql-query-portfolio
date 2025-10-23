-- ============================================================================
-- PROJECT: INTELLIGENT STOCK ALLOCATION SYSTEM
-- ============================================================================
-- Author: Aine Gradisher
-- GitHub: https://github.com/ainegradisher/sql-query-portfolio
-- Created: October 2025
-- 
-- ============================================================================
-- SCHEMA DISCLAIMER:
-- ============================================================================
-- The database schema shown (table names, columns, relationships) is a 
-- GENERIC, ANONYMIZED representation created for portfolio demonstration.
-- 
-- This structure:
-- • Does NOT represent any specific company's actual database
-- • Uses common naming conventions similar to standard ERP systems
-- • Is designed to demonstrate SQL techniques, not reproduce proprietary schemas
-- • Has been modified from original work to remove all identifying information
-- 
-- The VALUE of this portfolio piece is the PROBLEM-SOLVING APPROACH and
-- TECHNICAL IMPLEMENTATION, not the specific schema design.
-- ============================================================================
-- ============================================================================
-- PURPOSE: Automated stock distribution across customer orders using priority 
--          ranking based on promised delivery dates and real-time stock levels
-- 
-- BUSINESS PROBLEM SOLVED:
--   - Manual stock allocation took 3-4 hours daily
--   - Inconsistent decisions across different planners
--   - Urgent deadlines sometimes missed due to manual errors
--   - Gave planners ability to prioritise stock easily and quickly
--
-- TECHNICAL APPROACH:
--   - Uses Common Table Expressions (CTEs) for modular, readable logic
--   - Window functions for running totals and priority ranking
--   - Multi-currency handling with exchange rate conversions
--   - Waterfall allocation algorithm (highest priority gets stock first)
--
-- BUSINESS IMPACT:
--   - Reduced reporting time from 3-4 hours to <5 minutes daily
--   - £17,000 in admin tasks saved
--   - 100% consistent allocation logic
--   - Zero data errors and missed deadlines
--
-- TECHNICAL REQUIREMENTS:
--   - SQL Server 2016+ (or any RDBMS supporting window functions)
--   - No external dependencies
--   - Typical execution time: 2-3 seconds for 10,000 order lines
--
-- KEY SQL TECHNIQUES DEMONSTRATED:
--   ✓ Common Table Expressions (CTEs)
--   ✓ Window Functions (ROW_NUMBER, SUM OVER)
--   ✓ Complex Multi-Table Joins
--   ✓ Running Totals with Custom Frame Clauses
--   ✓ Advanced CASE Logic
--   ✓ Currency Conversion Handling
--   ✓ NULL Safety (ISNULL, NULLIF)
-- ============================================================================

-- ============================================================================
-- CTE 1: NOMINAL ACCOUNT DEDUPLICATION
-- ============================================================================
-- PROBLEM: Source system contains duplicate nominal account entries with same
--          account number but different IDs (data quality issue)
-- SOLUTION: Select the minimum ID for each unique account number/name pair
-- IMPACT: Ensures 1:1 relationship for joins, prevents duplicate rows
-- ============================================================================
WITH UniqueNominalAccounts AS (
    SELECT 
        MIN(NLNominalAccountID) as NLNominalAccountID,  -- Take first occurrence
        AccountNumber,                                   -- Financial account code
        AccountName                                      -- Account description
    FROM dbo.NLNominalAccount
    GROUP BY AccountNumber, AccountName                  -- Deduplicate on both fields
),

-- ============================================================================
-- CTE 2: REAL-TIME STOCK LEVEL CALCULATION
-- ============================================================================
-- PURPOSE: Calculate current available stock by item code
-- LOGIC: Opening stock minus all issued stock = current available
-- NOTE: Uses MovementBalance table which tracks all stock movements
-- ============================================================================
StockLevels AS (
    SELECT 
        SI.Code,                                                      -- Item/SKU code
        SUM(MB.OpeningStockLevel) - SUM(MB.StockLevelIssued) AS CurrentStockLevel
        -- Opening balance minus issued = what's actually available now
    FROM MovementBalance MB
    INNER JOIN StockItem SI ON MB.ItemID = SI.ItemID
    GROUP BY SI.Code                                                  -- One row per item
),

-- ============================================================================
-- CTE 3: ORDER LINES WITH PRIORITY RANKING
-- ============================================================================
-- PURPOSE: Build comprehensive dataset of all outstanding order lines
--          and assign priority ranking for stock allocation
-- 
-- PRIORITY LOGIC:
--   1. Already-allocated orders are deprioritized (rank last)
--   2. Then sort by promised delivery date (earliest first)
--   3. Break ties using document date (older orders first)
--
-- BUSINESS RULE: "First promised, first served"
--
-- EXAMPLE:
--   Order A: Promised Oct 25 → Priority 1
--   Order B: Promised Oct 26 → Priority 2
--   Order C: Promised Oct 27 → Priority 3
-- ============================================================================
OrderLinesWithPriority AS (
    SELECT
        -- =====================================================================
        -- IDENTIFIERS
        -- =====================================================================
        OrderLine.OrderLineID AS LineID,                       -- Unique line identifier
        OrderLine.PrintSequenceNumber AS LineNumber,           -- Line number within order
        OrderHeader.DocumentNo,                                -- Human-readable order number
        OrderHeader.CustomerDocumentNo,                        -- Customer's PO number
        
        -- =====================================================================
        -- CUSTOMER INFORMATION
        -- =====================================================================
        Customer.CustomerAccountNumber,                        -- Customer code
        Customer.CustomerAccountName,                          -- Customer name
        
        -- =====================================================================
        -- PRODUCT INFORMATION
        -- =====================================================================
        OrderLineView.ItemCode,                                -- SKU/Product code
        OrderLineView.ItemDescription,                         -- Product description
        
        -- =====================================================================
        -- QUANTITY TRACKING
        -- =====================================================================
        OrderLineView.LineQuantity AS Quantity,                -- Total quantity ordered
        OrderLine.DespatchReceiptQuantity,                     -- Qty already shipped
        OrderLine.InvoiceCreditQuantity,                       -- Qty already invoiced
        OrderLine.LineQuantity - OrderLine.DespatchReceiptQuantity AS QuantityDue,
        -- QuantityDue = what customer is still waiting for
        
        OrderLine.AllocatedQuantity,                           -- Qty already allocated (reserved)
        ISNULL(SL.CurrentStockLevel, 0) AS TotalStockLevel,   -- Current stock (0 if not found)
        
        -- =====================================================================
        -- FINANCIAL INFORMATION
        -- =====================================================================
        OrderLineView.UnitSellingPrice,                        -- Price per unit
        OrderLineView.LineTotalValue,                          -- Total line value
        Currency.Symbol AS CurrencySymbol,                     -- £, €, $ etc.
        Currency.CurrencyID,                                   -- Currency ID for conversion logic
        Currency.OneUnitBaseEquals,                            -- System exchange rate
        OrderHeader.ExchangeRate,                              -- Transaction exchange rate
        
        -- =====================================================================
        -- DATE TRACKING
        -- =====================================================================
        OrderHeader.DocumentDate,                              -- When order was placed
        OrderLine.RequestedDeliveryDate,                       -- When customer asked for
        OrderLine.PromisedDeliveryDate,                        -- When we promised to deliver
        
        -- =====================================================================
        -- REFERENCE FIELDS
        -- =====================================================================
        OrderHeader.SourceDocumentNo,                          -- Original quote/invoice ref
        OrderLine.NominalAccountRef,                           -- Financial account code
        OrderLine.LineTypeID,                                  -- 0=Stock, 1=Text, 2=Charge, 3=Comment
        OrderHeader.DocumentStatusId,                          -- 0=Live, 1=Hold, 2=Complete, etc.
        OrderHeader.DocumentTypeID,                            -- 0=Sales Order, 1=Return
        NL.AccountName AS NominalAccountName,                  -- Readable account name
        
        -- =====================================================================
        -- STOCK ALLOCATION LOGIC
        -- =====================================================================
        -- Calculate how much of this line should participate in stock distribution
        CASE 
            WHEN OrderLine.AllocatedQuantity > 0 THEN 0        
            -- Already allocated = don't include in new distribution
            ELSE OrderLine.LineQuantity - OrderLine.DespatchReceiptQuantity
            -- Not yet allocated = full outstanding quantity is eligible
        END AS EligibleQuantityDue,
        
        -- =====================================================================
        -- PRIORITY RANKING ALGORITHM
        -- =====================================================================
        -- ROW_NUMBER creates a unique sequential number within each item
        ROW_NUMBER() OVER (
            PARTITION BY OrderLineView.ItemCode                
            -- Separate priority queue for each product
            -- (stock of item A doesn't affect item B)
            ORDER BY 
                CASE WHEN OrderLine.AllocatedQuantity > 0 THEN 1 ELSE 0 END,
                -- Push already-allocated orders to bottom of priority list
                OrderLine.PromisedDeliveryDate ASC,            
                -- Earliest promised delivery date gets highest priority
                OrderHeader.DocumentDate ASC                   
                -- If promised dates are equal, older order wins
        ) AS PriorityRank
        -- RESULT: PriorityRank=1 is highest priority, 
        --         PriorityRank=2 is second, etc.
        
    FROM 
        -- =====================================================================
        -- TABLE JOINS
        -- =====================================================================
        CustomerAccount Customer
        INNER JOIN SalesOrderHeader OrderHeader 
            ON Customer.CustomerAccountID = OrderHeader.CustomerID
        INNER JOIN SalesOrderLine OrderLine 
            ON OrderHeader.OrderHeaderID = OrderLine.OrderHeaderID
        INNER JOIN SalesOrderLineView OrderLineView 
            ON OrderHeader.OrderHeaderID = OrderLineView.OrderHeaderID 
            AND OrderLine.PrintSequenceNumber = OrderLineView.PrintSequenceNumber
            -- Join on both fields to ensure exact line match
        INNER JOIN SystemCurrency Currency 
            ON Customer.CurrencyID = Currency.CurrencyID
        INNER JOIN UniqueNominalAccounts NL 
            ON OrderLine.NominalAccountRef = NL.AccountNumber
            -- Use deduplicated nominal accounts from CTE 1
        LEFT JOIN StockLevels SL 
            ON OrderLineView.ItemCode = SL.Code
            -- LEFT JOIN because not all items may have stock records
            
    WHERE 
        OrderHeader.DocumentTypeID <= 1                        -- Sales orders and returns only
        AND OrderHeader.DocumentStatusId <> 2                  -- Exclude completed orders
        AND OrderLine.LineQuantity > OrderLine.DespatchReceiptQuantity
        -- Only include lines with outstanding quantities
),

-- ============================================================================
-- CTE 4: RUNNING TOTAL CALCULATION (WATERFALL LOGIC)
-- ============================================================================
-- PURPOSE: Calculate cumulative demand as we work down the priority list
-- 
-- WHY WE NEED THIS:
--   If we have 100 units in stock and 5 orders:
--   - Order 1 (priority 1) needs 30 units  → Running total = 30
--   - Order 2 (priority 2) needs 40 units  → Running total = 70
--   - Order 3 (priority 3) needs 50 units  → Running total = 120 (exceeds stock!)
--   - Order 4 (priority 4) needs 20 units  → Running total = 140
--   - Order 5 (priority 5) needs 10 units  → Running total = 150
--
-- RESULT: Orders 1-2 get full allocation, Order 3 gets partial (30 units),
--         Orders 4-5 get nothing (stock exhausted)
--
-- TECHNICAL NOTE:
--   Window functions with ROWS clause allow us to calculate running totals
--   efficiently without self-joins or cursors
-- ============================================================================
StockDistribution AS (
    SELECT
        *,  -- Include all columns from previous CTE
        
        -- =====================================================================
        -- RUNNING TOTAL: CUMULATIVE DEMAND
        -- =====================================================================
        SUM(EligibleQuantityDue) OVER (
            PARTITION BY ItemCode                              -- Separate calculation per item
            ORDER BY PriorityRank                              -- Follow priority order
            ROWS UNBOUNDED PRECEDING                           -- Include all rows from start to current
        ) AS RunningQuantityDue,
        -- EXAMPLE: If priorities 1,2,3 need 10,20,30 units, 
        --          running totals are 10,30,60
        
        -- =====================================================================
        -- RUNNING TOTAL: DEMAND BEFORE CURRENT ROW
        -- =====================================================================
        ISNULL(SUM(EligibleQuantityDue) OVER (
            PARTITION BY ItemCode 
            ORDER BY PriorityRank 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING   
            -- All previous rows, but NOT current row
        ), 0) AS PreviousRunningQuantityDue
        -- EXAMPLE: For priority 3, this shows demand from priorities 1-2 only
        -- USED FOR: "How much stock is already claimed by higher priorities?"
        -- ISNULL handles first row (which has no previous rows) → returns 0
        
    FROM OrderLinesWithPriority
),

-- ============================================================================
-- CTE 5: FINAL STOCK ALLOCATION LOGIC
-- ============================================================================
-- PURPOSE: Determine exactly how much stock each order line should receive
--
-- ALLOCATION ALGORITHM:
--   IF line already has allocation 
--      THEN 0 (don't double-allocate)
--   ELSE IF all stock consumed by higher priorities 
--      THEN 0 (nothing left)
--   ELSE IF enough stock for this line and all higher priorities 
--      THEN full allocation
--   ELSE 
--      partial allocation (whatever's left)
--
-- EXAMPLE SCENARIO:
--   Available Stock: 100 units
--   Priority 1: Needs 30 → Gets 30 (100-0=30)
--   Priority 2: Needs 40 → Gets 40 (100-30=40)
--   Priority 3: Needs 50 → Gets 30 (100-70=30, only 30 left!)
--   Priority 4: Needs 20 → Gets 0 (100-100=0, nothing left)
-- ============================================================================
FinalAllocation AS (
    SELECT
        *,  -- Include all columns from previous CTE
        
        -- =====================================================================
        -- PROPOSED ALLOCATION CALCULATION
        -- =====================================================================
        CASE
            -- SCENARIO 1: Already allocated → don't allocate more
            WHEN AllocatedQuantity > 0 THEN 0  
            
            -- SCENARIO 2: Stock exhausted by higher priorities
            WHEN TotalStockLevel <= PreviousRunningQuantityDue THEN 0  
            -- EXAMPLE: 100 units in stock, but priority 1-5 already claimed 120 
            --          → nothing left
            
            -- SCENARIO 3: Full allocation possible
            WHEN TotalStockLevel >= RunningQuantityDue THEN EligibleQuantityDue  
            -- EXAMPLE: 100 units in stock, priorities 1-current only need 80 
            --          → can fulfill completely
            
            -- SCENARIO 4: Partial allocation (stock runs out partway through this line)
            ELSE GREATEST(TotalStockLevel - PreviousRunningQuantityDue, 0)
            -- EXAMPLE: 100 in stock, previous priorities claimed 90, current needs 30
            --          → Can only allocate 10 (100-90)
            -- GREATEST ensures we never return negative numbers
        END AS ProposedStockAllocation,
        
        -- =====================================================================
        -- TOTAL DISTRIBUTED STOCK PER ITEM
        -- =====================================================================
        -- Sum of all proposed allocations for this item (used to calculate leftovers)
        SUM(CASE
            WHEN AllocatedQuantity > 0 THEN 0
            WHEN TotalStockLevel <= PreviousRunningQuantityDue THEN 0
            WHEN TotalStockLevel >= RunningQuantityDue THEN EligibleQuantityDue
            ELSE GREATEST(TotalStockLevel - PreviousRunningQuantityDue, 0)
        END) OVER (PARTITION BY ItemCode) AS TotalDistributedStock
        -- Window function without ORDER BY = calculates total across all rows for each item
        
    FROM StockDistribution
)

-- ============================================================================
-- FINAL OUTPUT: BUSINESS-READY STOCK ALLOCATION REPORT
-- ============================================================================
-- PURPOSE: Use technical calculations to action stock issues
-- AUDIENCE: Warehouse managers, sales team, operations planners
-- USAGE: Daily stock allocation meetings, customer promise updates
--
-- KEY OUTPUT FIELDS:
--   - Proposed Allocation: What warehouse should pick
--   - Total Available to Ship: What sales can promise to customer
--   - Proposed Status: Will this fulfill the order?
--   - Value Available to Ship: Revenue impact
-- ============================================================================
SELECT
    -- =========================================================================
    -- SECTION 1: ORDER IDENTIFICATION
    -- =========================================================================
    LineID AS 'Order Line ID',
    LineNumber AS 'Line Number',
    DocumentNo AS 'Document No',
    CustomerDocumentNo AS 'Customer Order No',
    CustomerAccountNumber AS 'Customer Code',
    CustomerAccountName AS 'Customer Name',
    
    -- =========================================================================
    -- SECTION 2: PRODUCT INFORMATION
    -- =========================================================================
    ItemCode AS 'Item Code',
    ItemDescription AS 'Description',
    
    -- =========================================================================
    -- SECTION 3: QUANTITY ANALYSIS
    -- =========================================================================
    Quantity AS 'Quantity Ordered',
    DespatchReceiptQuantity AS 'Despatched Quantity',
    InvoiceCreditQuantity AS 'Invoiced Quantity',
    QuantityDue AS 'Quantity Outstanding',
    AllocatedQuantity AS 'Currently Allocated',
    
    -- =========================================================================
    -- SECTION 4: STOCK ALLOCATION RECOMMENDATIONS - KEY SECTION
    -- =========================================================================
    -- This is the actionable output section that drives daily decisions
    
    TotalStockLevel AS 'Current Stock Level',
    
    ProposedStockAllocation AS 'Proposed Allocation',
    -- KEY FIELD: This is what warehouse should pick
    
    AllocatedQuantity + ProposedStockAllocation AS 'Total Available to Ship',
    -- KEY FIELD: This is what sales can promise to customer
    
    (AllocatedQuantity + ProposedStockAllocation) * (UnitSellingPrice / ExchangeRate) 
        AS 'Value Available to Ship (GBP)',
    -- Financial impact: how much revenue can we recover?
    
    TotalStockLevel - TotalDistributedStock AS 'Stock Remaining After Allocation',
    -- Leftover stock after all allocations (for purchasing decisions)
    
    -- =========================================================================
    -- SECTION 5: STATUS INDICATORS
    -- =========================================================================
    -- Human-readable status labels for quick scanning
    
    -- Current allocation status (before this algorithm runs)
    CASE 
        WHEN AllocatedQuantity + DespatchReceiptQuantity = Quantity 
        THEN 'Fully Allocated'
        WHEN AllocatedQuantity + DespatchReceiptQuantity > 0 
        THEN 'Partially Allocated'
        ELSE 'Not Allocated'
    END AS 'Current Status',
    
    -- Projected status (after applying proposed allocations)
    CASE 
        WHEN AllocatedQuantity + ProposedStockAllocation + DespatchReceiptQuantity >= Quantity 
        THEN 'Would be Fully Allocated'
        WHEN AllocatedQuantity + ProposedStockAllocation + DespatchReceiptQuantity > 0 
        THEN 'Would be Partially Allocated'
        ELSE 'Would Remain Unallocated'
    END AS 'Proposed Status',
    
    -- =========================================================================
    -- SECTION 6: FINANCIAL INFORMATION
    -- =========================================================================
    -- Multi-currency handling for international orders
    
    UnitSellingPrice AS 'Unit Price (Local Currency)',
    LineTotalValue AS 'Net Value (Local Currency)',
    CurrencySymbol AS 'Currency',
    ExchangeRate AS 'Exchange Rate',
    
    -- Convert to GBP for consolidated reporting
    UnitSellingPrice / ExchangeRate AS 'Unit Price (GBP)',
    (UnitSellingPrice / ExchangeRate) * Quantity AS 'Total Order Value (GBP)',
    QuantityDue * (UnitSellingPrice / ExchangeRate) AS 'Outstanding Value (GBP)',
    DespatchReceiptQuantity * (UnitSellingPrice / ExchangeRate) AS 'Despatched Value (GBP)',
    
    -- =========================================================================
    -- SECTION 7: SYSTEM FX RATE (FOR RECONCILIATION)
    -- =========================================================================
    -- Used for financial reconciliation between sales and accounting systems
    -- Handles different base currencies (GBP, EUR, etc.)
    CASE 
        WHEN CurrencyID = 1 THEN UnitSellingPrice  -- Already in GBP
        WHEN CurrencyID = 2 THEN UnitSellingPrice * 
             (SELECT OneEuroEquals FROM dbo.SystemCurrency WHERE CurrencyID = 1)
             -- Convert EUR to GBP using system rate
        ELSE UnitSellingPrice * (1 / NULLIF(OneUnitBaseEquals, 0)) * 
             (SELECT OneEuroEquals FROM dbo.SystemCurrency WHERE CurrencyID = 1)
             -- Convert other currency → EUR → GBP
             -- NULLIF prevents division by zero errors
    END AS 'Unit Price (System FX Rate)',
    
    CASE 
        WHEN CurrencyID = 1 THEN LineTotalValue
        WHEN CurrencyID = 2 THEN LineTotalValue * 
             (SELECT OneEuroEquals FROM dbo.SystemCurrency WHERE CurrencyID = 1)
        ELSE LineTotalValue * (1 / NULLIF(OneUnitBaseEquals, 0)) * 
             (SELECT OneEuroEquals FROM dbo.SystemCurrency WHERE CurrencyID = 1)
    END AS 'Net Value (System FX Rate)',
    
    -- =========================================================================
    -- SECTION 8: DATE INFORMATION
    -- =========================================================================
    DocumentDate AS 'Order Date',
    RequestedDeliveryDate AS 'Requested Delivery',
    PromisedDeliveryDate AS 'Promised Delivery',
    -- KEY FIELD: This drives the priority ranking
    SourceDocumentNo AS 'Source Quote/Invoice',
    
    -- =========================================================================
    -- SECTION 9: OPERATIONAL STATUS
    -- =========================================================================
    CASE 
        WHEN DespatchReceiptQuantity = Quantity THEN 'Fully Despatched'
        WHEN DespatchReceiptQuantity < 1 THEN 'Not Despatched'
        ELSE 'Partially Despatched'
    END AS 'Despatch Status',
    
    -- =========================================================================
    -- SECTION 10: ACCOUNTING REFERENCE
    -- =========================================================================
    NominalAccountRef AS 'Nominal Code',
    NominalAccountName AS 'Account Name',
    
    -- =========================================================================
    -- SECTION 11: TECHNICAL METADATA
    -- =========================================================================
    CASE 
        WHEN LineTypeID = 0 THEN 'Stock Item'
        WHEN LineTypeID = 1 THEN 'Free Text'
        WHEN LineTypeID = 2 THEN 'Additional Charge'
        WHEN LineTypeID = 3 THEN 'Comment Line'
        ELSE 'Unknown'
    END AS 'Line Type',
    
    CASE
        WHEN DocumentStatusId = 0 THEN 'Live'
        WHEN DocumentStatusId = 1 THEN 'On Hold'
        WHEN DocumentStatusId = 2 THEN 'Completed'
        WHEN DocumentStatusId = 3 THEN 'Dispute'
        WHEN DocumentStatusId = 4 THEN 'Cancelled'
        ELSE 'Unknown'
    END AS 'Order Status',
    
    CASE
        WHEN DocumentTypeID = 0 THEN 'Sales Order'
        WHEN DocumentTypeID = 1 THEN 'Sales Return'
    END AS 'Document Type',
    
    -- =========================================================================
    -- SECTION 12: ANALYSIS FIELDS
    -- =========================================================================
    PriorityRank AS 'Allocation Priority Rank'
    -- Lower number = higher priority for stock allocation
    
FROM FinalAllocation

-- ============================================================================
-- FINAL SORTING
-- ============================================================================
ORDER BY 
    ItemCode,           -- Group by product
    PriorityRank,       -- Show highest priority first
    DocumentDate DESC;  -- Break ties with newest orders first

-- ============================================================================
-- PERFORMANCE NOTES:
-- ============================================================================
-- - CTEs are optimised by SQL Server (not materialised unless needed)
-- - Recommended indexes:
--   * ItemCode (frequently used in PARTITION BY)
--   * CustomerID (join key)
--   * PromisedDeliveryDate (used in ORDER BY within window function)
--   * DocumentStatusId (used in WHERE clause)
-- - Typical execution time: 2-3 seconds for ~10,000 order lines
-- - Memory usage: Moderate (window functions require sort operations)
-- - Scalability: Tested up to 50,000 order lines without performance degradation
--
-- ============================================================================
-- BUSINESS IMPACT SUMMARY:
-- ============================================================================
-- BEFORE IMPLEMENTATION:
--   Time: 3-4 hours daily manual allocation
--   Consistency: Variable (different planners, different decisions)
--   Audit Trail: None (decisions in people's heads)
--   Customer Impact: Frequent complaints about missed delivery promises
--   Error Rate: ~5% data entry errors
--
-- AFTER IMPLEMENTATION:
--    Time: <5 minutes daily (automated)
--    Consistency: 100% (same logic every time)
--    Audit Trail: Complete (can trace every decision)
--    Customer Impact: Reduced complaints by prioritizing urgent deadlines
--    Error Rate: 0% (no manual data entry)
--   - Reduced expedited shipping costs (better planning = less rush shipping)
--   - Freed up planner time for value-add analysis vs. manual data entry
--
-- ============================================================================
-- END OF FILE
-- ============================================================================
