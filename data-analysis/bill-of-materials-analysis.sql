/*
=============================================================================
BILL OF MATERIALS (BOM) EXPLOSION QUERY
=============================================================================
Purpose: Recursively traverse product hierarchy to show all components and
         sub-components at every level of assembly
         
Business Use: Manufacturing planning, cost analysis, inventory requirements
Technical Skills: Recursive CTEs, hierarchical data, string manipulation
Author: Aine Gradisher
Date: 2025
=============================================================================
*/

WITH bom_explosion AS (
    
    -- ============================================================
    -- ANCHOR MEMBER: Starting point of recursion
    -- ============================================================
    -- Get all direct components of the parent product
    SELECT 
        parent.ProductCode AS ParentProductCode,
        component.PartNumber AS ComponentPartNumber,
        component.Description AS ComponentDescription,
        component.ComponentType,
        component.QuantityRequired,
        1 AS HierarchyLevel,                                      -- Track depth in BOM structure
        CAST(component.PartNumber AS VARCHAR(4000)) AS BomPath    -- Track full path for ordering
    FROM ProductMaster parent
    INNER JOIN ProductStructure ps ON parent.ProductID = ps.ProductID
    INNER JOIN ComponentDetails component ON ps.StructureID = component.StructureID
    WHERE parent.ProductCode = 'PRODUCT-12345'                    -- *** PARAMETER: Change to desired product ***
      AND ps.StructureType = 0                                    -- Active production structure
      AND ps.IsActive = 1                                         -- Only active BOMs
    
    UNION ALL
    
    -- ============================================================
    -- RECURSIVE MEMBER: Find components of components
    -- ============================================================
    -- Each iteration goes one level deeper into the BOM hierarchy
    SELECT 
        be.ComponentPartNumber AS ParentProductCode,              -- Previous component becomes new parent
        component.PartNumber AS ComponentPartNumber,
        component.Description AS ComponentDescription,
        component.ComponentType,
        component.QuantityRequired,
        be.HierarchyLevel + 1,                                    -- Increment depth counter
        CAST(be.BomPath + ' -> ' + component.PartNumber AS VARCHAR(4000))  -- Append to path string
    FROM bom_explosion be                                         -- Self-reference to CTE
    INNER JOIN ProductMaster parent ON be.ComponentPartNumber = parent.ProductCode
    INNER JOIN ProductStructure ps ON parent.ProductID = ps.ProductID
    INNER JOIN ComponentDetails component ON ps.StructureID = component.StructureID
    WHERE be.HierarchyLevel < 20                                  -- Safety limit to prevent infinite loops
      AND ps.StructureType = 0
      AND ps.IsActive = 1
)

-- ============================================================
-- FINAL OUTPUT: Format and display BOM hierarchy
-- ============================================================
SELECT 
    HierarchyLevel AS Level,
    REPLICATE('  ', HierarchyLevel - 1) + ComponentPartNumber AS Component,  -- Indent by level for visual hierarchy
    ComponentDescription AS Description,
    QuantityRequired AS Qty,
    BomPath AS FullPath                                           -- Shows complete lineage
FROM bom_explosion
ORDER BY BomPath;                                                 -- Sort maintains parent-child relationship

/*
=============================================================================
QUERY EXPLANATION
=============================================================================

WHAT IT DOES:
This recursive query "explodes" a Bill of Materials to show every component,
sub-component, and sub-sub-component in a product assembly.

HOW IT WORKS:
1. ANCHOR: Starts with top-level product and finds immediate components
2. RECURSION: Each component is treated as a new parent, finding its components
3. TRACKING: Maintains hierarchy level and full path for each component
4. TERMINATION: Stops at 20 levels deep (configurable safety limit)
5. OUTPUT: Displays indented structure showing assembly relationships

KEY SQL TECHNIQUES:
- Recursive Common Table Expression (CTE)
- Self-referencing joins
- Hierarchical path tracking with string concatenation
- CAST for data type management in concatenation
- REPLICATE for visual formatting
- Level-based recursion control

BUSINESS VALUE:
- Cost rollup calculations
- Material requirements planning (MRP)
- Lead time analysis
- "Where used" reporting
- Engineering change impact assessment

EXAMPLE OUTPUT:
Level | Component       | Description           | Qty | FullPath
------|-----------------|----------------------|-----|-------------------
1     | COMP-001        | Main Assembly        | 1   | COMP-001
2     |   COMP-002      | Sub-assembly A       | 2   | COMP-001 -> COMP-002
3     |     COMP-003    | Part X               | 4   | COMP-001 -> COMP-002 -> COMP-003
2     |   COMP-004      | Sub-assembly B       | 1   | COMP-001 -> COMP-004

PERFORMANCE NOTES:
- Recursion depth limit prevents runaway queries
- Active structure filtering reduces unnecessary data
- Path tracking enables correct ordering without additional sorts
- Consider indexing on ProductCode and StructureID for large datasets

ADAPTATIONS:
- Change depth limit (Level < 20) based on your product complexity
- Add quantity multiplication for total material requirements
- Join to inventory/cost tables for extended analytics
- Filter ComponentType for specific material categories

=============================================================================
*/
