CREATE OR REPLACE TABLE <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup
USING DELTA AS
SELECT DISTINCT
    lookup.tenant_id,
    lookup.GlobalResourceGroupID,
    lookup.GlobalResourceID,
    lookup.DisplayMember,
    lookup.Code,
    lookup.Category
FROM (
SELECT
    gr.tenant_id,
    gr.GlobalResourceGroupID,
    gr.GlobalResourceID,
    COALESCE(gro.DisplayMember, grrso.DisplayMember, gr.DisplayMember) AS DisplayMember,
    CAST(NULL AS STRING) AS Code,
    grt.Label AS Category
FROM <common_bronze_schema>.globalresource_globalresource AS gr
LEFT JOIN <common_bronze_schema>.globalresource_globalresourceoverride AS gro
    ON gr.tenant_id = gro.tenant_id
   AND gr.GlobalResourceID = gro.GlobalResourceID
   AND gro.ReportingStandardID = 20
   AND gro.AgencyID = '0bdffbd6-5502-4392-b67d-86b477954186'
LEFT JOIN <common_bronze_schema>.globalresource_globalresourcereportingstandardoverride AS grrso
    ON gr.tenant_id = grrso.tenant_id
   AND gr.GlobalResourceID = grrso.GlobalResourceID
   AND grrso.ReportingStandardID = 20
LEFT JOIN <common_bronze_schema>.globalresource_globalresourcetreeglobalresource AS grtgr
    ON gr.tenant_id = grtgr.tenant_id
   AND gr.GlobalResourceID = grtgr.GlobalResourceID
LEFT JOIN <common_bronze_schema>.globalresource_globalresourcetree AS grt
    ON gr.tenant_id = grt.tenant_id
   AND gr.GlobalResourceGroupID = grt.GlobalResourceGroupID
   AND grtgr.GlobalResourceTreeID = grt.GlobalResourceTreeID
WHERE gr.GlobalResourceGroupID IN (
    'ApparatusorResourceType',
    'PersonnelAttended',
    'PersonnelLevel',
    'EMSPersonnelEmploymentStatus',
    'PersonnelActionsTaken',
    'PersonnelRole'
)

UNION ALL

SELECT
    poc.tenant_id,
    gr.GlobalResourceGroupID,
    poc.PlusOneCodeID AS GlobalResourceID,
    COALESCE(poc.Label, poc.InitialLabel) AS DisplayMember,
    poc.Code,
    grt.Label AS Category
FROM <common_bronze_schema>.globalresource_globalresource AS gr
INNER JOIN <common_bronze_schema>.globalresource_plusonecode AS poc
    ON gr.tenant_id = poc.tenant_id
   AND gr.GlobalResourceID = poc.GlobalResourceID
   AND poc.DeletedStatus = False
LEFT JOIN <common_bronze_schema>.globalresource_globalresourcetreeglobalresource AS grtgr
    ON gr.tenant_id = grtgr.tenant_id
   AND gr.GlobalResourceID = grtgr.GlobalResourceID
LEFT JOIN <common_bronze_schema>.globalresource_globalresourcetree AS grt
    ON gr.tenant_id = grt.tenant_id
   AND gr.GlobalResourceGroupID = grt.GlobalResourceGroupID
   AND grtgr.GlobalResourceTreeID = grt.GlobalResourceTreeID
WHERE gr.GlobalResourceGroupID IN (
    'ApparatusorResourceType',
    'PersonnelAttended',
    'PersonnelLevel',
    'EMSPersonnelEmploymentStatus',
    'PersonnelActionsTaken',
    'PersonnelRole'
)
) AS lookup;

CREATE OR REPLACE TEMP VIEW vw_dim_apparatuspersonnel_nv_lookup AS
SELECT DISTINCT
    lookup.tenant_id,
    lookup.GlobalResourceGroupID,
    lookup.GlobalResourceID,
    lookup.DisplayMember
FROM (
SELECT
    grgnv.tenant_id,
    grgnv.GlobalResourceGroupID,
    grgnv.GlobalResourceGroupNotValueID AS GlobalResourceID,
    COALESCE(nvo.Value, nvrso.Value, nv.Value) AS DisplayMember
FROM <common_bronze_schema>.globalresource_globalresourcegroupnotvalue AS grgnv
INNER JOIN <common_bronze_schema>.globalresource_notvalue AS nv
    ON grgnv.tenant_id = nv.tenant_id
   AND grgnv.NotValueID = nv.NotValueID
LEFT JOIN <common_bronze_schema>.globalresource_notvalueoverride AS nvo
    ON grgnv.tenant_id = nvo.tenant_id
   AND grgnv.GlobalResourceGroupNotValueID = nvo.GlobalResourceGroupNotValueID
   AND nvo.ReportingStandardID = 20
   AND nvo.AgencyID = '0bdffbd6-5502-4392-b67d-86b477954186'
LEFT JOIN <common_bronze_schema>.globalresource_notvaluereportingstandardoverride AS nvrso
    ON grgnv.tenant_id = nvrso.tenant_id
   AND grgnv.GlobalResourceGroupNotValueID = nvrso.GlobalResourceGroupNotValueID
   AND nvrso.ReportingStandardID = 20
WHERE grgnv.GlobalResourceGroupID IN (
    'ApparatusorResourceType',
    'EMSPersonnelEmploymentStatus'
)

UNION ALL

SELECT
    poc.tenant_id,
    grgnv.GlobalResourceGroupID,
    poc.PlusOneCodeID AS GlobalResourceID,
    COALESCE(poc.Label, poc.InitialLabel) AS DisplayMember
FROM <common_bronze_schema>.globalresource_globalresourcegroupnotvalue AS grgnv
INNER JOIN <common_bronze_schema>.globalresource_plusonecode AS poc
    ON grgnv.tenant_id = poc.tenant_id
   AND grgnv.GlobalResourceGroupNotValueID = poc.GlobalResourceGroupNotValueID
WHERE grgnv.GlobalResourceGroupID IN (
    'ApparatusorResourceType',
    'EMSPersonnelEmploymentStatus'
)
) AS lookup;

CREATE OR REPLACE TEMP VIEW vw_dim_apparatuspersonnel_field_mapping AS
SELECT
    fmt.FieldMappingTypeID,
    fm.OtherSystemField,
    fm.DefaultLabel,
    fm.ImportExportBindingPathEntryID AS BindingPathEntryID,
    fvm.tenant_id,
    fvm.ITValue,
    COALESCE(fvmrso.OtherSystemValue, fvmrso.FallbackValueForExtension, fvm.OtherSystemValue) AS OtherSystemValue,
    MAX(fvm.ModifiedOn) AS ModifiedOn
FROM <common_bronze_schema>.fieldmapping_fieldmappingtype AS fmt
INNER JOIN <common_bronze_schema>.fieldmapping_fieldmapping AS fm
    ON fmt.tenant_id = fm.tenant_id
   AND fmt.FieldMappingTypeID = fm.FieldMappingTypeID
   AND fm.FieldNodeID IS NOT NULL
INNER JOIN <common_bronze_schema>.fieldmapping_fieldvaluemapping AS fvm
    ON fm.tenant_id = fvm.tenant_id
   AND fm.FieldMappingID = fvm.FieldMappingID
   AND fvm.FieldValueMappingTypeID = 1
LEFT JOIN <common_bronze_schema>.fieldmapping_fieldvaluemappingreportingstandardoverride AS fvmrso
    ON fvm.tenant_id = fvmrso.tenant_id
   AND fvm.FieldValueMappingID = fvmrso.FieldValueMappingID
   AND fvmrso.ReportingStandardID = 20
WHERE fmt.FieldMappingTypeID = 5
GROUP BY
    fmt.FieldMappingTypeID,
    fm.OtherSystemField,
    fm.DefaultLabel,
    fm.ImportExportBindingPathEntryID,
    fvm.tenant_id,
    fvm.ITValue,
    COALESCE(fvmrso.OtherSystemValue, fvmrso.FallbackValueForExtension, fvm.OtherSystemValue);

CREATE OR REPLACE TEMP VIEW changed_dim_apparatuspersonnel_keys AS
SELECT DISTINCT
    keys.tenant_id,
    keys.ApparatusPersonnel_ID_Internal,
    keys.Apparatus_ID_Internal
FROM (
    SELECT
        ap.tenant_id,
        ap.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
    WHERE ap.DeletedStatus = False
      AND ap.<common_bronze_delta_placeholder>

    UNION ALL

    SELECT
        ap.tenant_id,
        ap.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <common_bronze_schema>.fireevent_apparatus AS a
    INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
        ON a.tenant_id = ap.tenant_id
       AND a.ApparatusID = ap.ApparatusID
       AND ap.DeletedStatus = False
    WHERE a.DeletedStatus = False
      AND a.<common_bronze_delta_placeholder>

    UNION ALL

    SELECT
        ap.tenant_id,
        ap.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
    INNER JOIN <source_bronze_schema>.fireevent_apparatustypemodvalue AS at
        ON ap.tenant_id = at.tenant_id
       AND ap.ApparatusID = at.ApparatusID
    WHERE ap.DeletedStatus = False
      AND at.<source_bronze_delta_placeholder>

    UNION ALL

    SELECT
        lvl.tenant_id,
        lvl.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <source_bronze_schema>.fireevent_apparatuspersonnellevelmodvalue AS lvl
    INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
        ON lvl.tenant_id = ap.tenant_id
       AND lvl.ApparatusPersonnelID = ap.ApparatusPersonnelID
       AND ap.DeletedStatus = False
    WHERE lvl.<source_bronze_delta_placeholder>

    UNION ALL

    SELECT
        ap.tenant_id,
        ap.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
    INNER JOIN <common_bronze_schema>.resource_payrate AS pr
        ON ap.tenant_id = pr.tenant_id
       AND ap.PayRateID = pr.PayRateID
       AND pr.<common_bronze_delta_placeholder>
    WHERE ap.DeletedStatus = False

    UNION ALL

    SELECT
        r.tenant_id,
        r.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <source_bronze_schema>.fireevent_apparatuspersonnelrolemodvalue AS r
    INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
        ON r.tenant_id = ap.tenant_id
       AND r.ApparatusPersonnelID = ap.ApparatusPersonnelID
       AND ap.DeletedStatus = False
    WHERE r.<source_bronze_delta_placeholder>

    UNION ALL

    SELECT
        pamv.tenant_id,
        pamv.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <source_bronze_schema>.fireevent_apparatuspersonnelactionprimaryactionmodvalue AS pamv
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelaction AS apa
        ON pamv.tenant_id = apa.tenant_id
       AND pamv.ApparatusPersonnelID = apa.ApparatusPersonnelID
    INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
        ON pamv.tenant_id = ap.tenant_id
       AND pamv.ApparatusPersonnelID = ap.ApparatusPersonnelID
       AND ap.DeletedStatus = False
    WHERE pamv.<source_bronze_delta_placeholder>

    UNION ALL

    SELECT
        apao.tenant_id,
        apao.ApparatusPersonnelID AS ApparatusPersonnel_ID_Internal,
        ap.ApparatusID AS Apparatus_ID_Internal
    FROM <source_bronze_schema>.fireevent_apparatuspersonnelactionother AS apao
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelactionotheractionmodvalue AS oamv
        ON apao.tenant_id = oamv.tenant_id
       AND apao.ApparatusPersonnelActionID = oamv.ApparatusPersonnelActionID
    INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
        ON apao.tenant_id = ap.tenant_id
       AND apao.ApparatusPersonnelID = ap.ApparatusPersonnelID
       AND ap.DeletedStatus = False
    WHERE oamv.<source_bronze_delta_placeholder>
) AS keys;

CREATE OR REPLACE TABLE <source_silver_schema>.stg_dim_apparatuspersonnel_base
USING DELTA AS
SELECT DISTINCT
    ids.tenant_id,
    ids.ApparatusPersonnel_ID_Internal,
    ids.Apparatus_ID_Internal,
    i.IncidentID AS Incident_ID_Internal,
    i.AgencyID,
    ap.IsAttended,
    ap.LicenseNumber,
    ap.LastName,
    ap.FirstName,
    ap.Rank,
    ap.Rate,
    ap.TimeIn,
    ap.TimeOut,
    ap.TimeSpent,
    ap.PayRateID,
    ap.PayRateDescription,
    ap.PerformerID
FROM changed_dim_apparatuspersonnel_keys AS ids
INNER JOIN <common_bronze_schema>.fireevent_apparatuspersonnel AS ap
    ON ids.tenant_id = ap.tenant_id
   AND ids.ApparatusPersonnel_ID_Internal = ap.ApparatusPersonnelID
   AND ids.Apparatus_ID_Internal = ap.ApparatusID
   AND ap.DeletedStatus = False
INNER JOIN <common_bronze_schema>.fireevent_apparatus AS a
    ON ap.tenant_id = a.tenant_id
   AND ap.ApparatusID = a.ApparatusID
   AND a.DeletedStatus = False
INNER JOIN <common_bronze_schema>.fireevent_incident AS i
    ON a.tenant_id = i.tenant_id
   AND a.IncidentID = i.IncidentID
   AND i.IncidentTypeID = 5
   AND i.DeletedStatus = False;

CREATE OR REPLACE TABLE <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked
USING DELTA AS
SELECT
    a.tenant_id,
    a.ApparatusPersonnel_ID_Internal,
    a.Category,
    a.DisplayMember,
    a.Code,
    ROW_NUMBER() OVER (
        PARTITION BY a.tenant_id, a.ApparatusPersonnel_ID_Internal
        ORDER BY a.Sort1, a.Sort2
    ) AS RowNumber
FROM (
    SELECT
        b.tenant_id,
        b.ApparatusPersonnel_ID_Internal,
        gr.Category,
        gr.DisplayMember,
        fm.OtherSystemValue AS Code,
        0 AS Sort1,
        pamv.CreatedOn AS Sort2
    FROM <source_silver_schema>.stg_dim_apparatuspersonnel_base AS b
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelaction AS apa
        ON b.tenant_id = apa.tenant_id
       AND b.ApparatusPersonnel_ID_Internal = apa.ApparatusPersonnelID
       AND apa.DeletedStatus = False
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelactionprimaryactionmodvalue AS pamv
        ON apa.tenant_id = pamv.tenant_id
       AND apa.ApparatusPersonnelID = pamv.ApparatusPersonnelID
       AND pamv.DeletedStatus = False
    INNER JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr
        ON pamv.tenant_id = gr.tenant_id
       AND COALESCE(pamv.PlusOneCode, pamv.PrimaryAction) = gr.GlobalResourceID
       AND gr.GlobalResourceGroupID = 'PersonnelActionsTaken'
    LEFT JOIN vw_dim_apparatuspersonnel_field_mapping AS fm
        ON gr.tenant_id = fm.tenant_id
       AND gr.GlobalResourceID = fm.ITValue

    UNION

    SELECT
        b.tenant_id,
        b.ApparatusPersonnel_ID_Internal,
        gr.Category,
        gr.DisplayMember,
        COALESCE(CONCAT(fm.OtherSystemValue, gr.Code), gr.Code, fm.OtherSystemValue) AS Code,
        1 AS Sort1,
        oamv.CreatedOn AS Sort2
    FROM <source_silver_schema>.stg_dim_apparatuspersonnel_base AS b
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelactionother AS apao
        ON b.tenant_id = apao.tenant_id
       AND b.ApparatusPersonnel_ID_Internal = apao.ApparatusPersonnelID
    INNER JOIN <source_bronze_schema>.fireevent_apparatuspersonnelactionotheractionmodvalue AS oamv
        ON apao.tenant_id = oamv.tenant_id
       AND apao.ApparatusPersonnelActionID = oamv.ApparatusPersonnelActionID
       AND oamv.DeletedStatus = False
    INNER JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr
        ON oamv.tenant_id = gr.tenant_id
       AND COALESCE(oamv.PlusOneCode, oamv.Action) = gr.GlobalResourceID
       AND gr.GlobalResourceGroupID = 'PersonnelActionsTaken'
    LEFT JOIN vw_dim_apparatuspersonnel_field_mapping AS fm
        ON gr.tenant_id = fm.tenant_id
       AND gr.GlobalResourceID = fm.ITValue
) AS a;

CREATE OR REPLACE TEMP VIEW vw_dim_apparatuspersonnel_actions_pivot AS
SELECT
    ids.tenant_id,
    ids.ApparatusPersonnel_ID_Internal,
    a1.DisplayMember AS Apparatus_Personnel_Actions_Taken_1,
    a2.DisplayMember AS Apparatus_Personnel_Actions_Taken_2,
    a3.DisplayMember AS Apparatus_Personnel_Actions_Taken_3,
    a4.DisplayMember AS Apparatus_Personnel_Actions_Taken_4,
    a1.Category AS Apparatus_Personnel_Actions_Taken_Category_1,
    a2.Category AS Apparatus_Personnel_Actions_Taken_Category_2,
    a3.Category AS Apparatus_Personnel_Actions_Taken_Category_3,
    a4.Category AS Apparatus_Personnel_Actions_Taken_Category_4,
    a1.Code AS Apparatus_Personnel_Actions_Taken_Code_1,
    a2.Code AS Apparatus_Personnel_Actions_Taken_Code_2,
    a3.Code AS Apparatus_Personnel_Actions_Taken_Code_3,
    a4.Code AS Apparatus_Personnel_Actions_Taken_Code_4,
    CONCAT(a1.Code, ' - ', a1.DisplayMember) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_1,
    CONCAT(a2.Code, ' - ', a2.DisplayMember) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_2,
    CONCAT(a3.Code, ' - ', a3.DisplayMember) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_3,
    CONCAT(a4.Code, ' - ', a4.DisplayMember) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_4
FROM (
    SELECT DISTINCT
        tenant_id,
        ApparatusPersonnel_ID_Internal
    FROM <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked
) AS ids
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked AS a1
    ON ids.tenant_id = a1.tenant_id
   AND ids.ApparatusPersonnel_ID_Internal = a1.ApparatusPersonnel_ID_Internal
   AND a1.RowNumber = 1
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked AS a2
    ON ids.tenant_id = a2.tenant_id
   AND ids.ApparatusPersonnel_ID_Internal = a2.ApparatusPersonnel_ID_Internal
   AND a2.RowNumber = 2
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked AS a3
    ON ids.tenant_id = a3.tenant_id
   AND ids.ApparatusPersonnel_ID_Internal = a3.ApparatusPersonnel_ID_Internal
   AND a3.RowNumber = 3
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_actions_ranked AS a4
    ON ids.tenant_id = a4.tenant_id
   AND ids.ApparatusPersonnel_ID_Internal = a4.ApparatusPersonnel_ID_Internal
   AND a4.RowNumber = 4;

CREATE OR REPLACE TEMP VIEW vw_dim_apparatuspersonnel_primary_email AS
SELECT
    tenant_id,
    PerformerID,
    EmailAddress
FROM (
    SELECT
        pea.tenant_id,
        pea.PerformerID,
        emv.EmailAddress,
        ROW_NUMBER() OVER (
            PARTITION BY pea.tenant_id, pea.PerformerID
            ORDER BY pea.IsPrimary DESC, pea.PerformerEmailAddressID
        ) AS rn
    FROM <common_bronze_schema>.resource_performeremailaddress AS pea
    INNER JOIN <common_bronze_schema>.resource_performeremailaddressemailaddressmodvalue AS emv
        ON pea.tenant_id = emv.tenant_id
       AND pea.PerformerEmailAddressID = emv.PerformerEmailAddressID
       AND LENGTH(emv.EmailAddress) >= 3
       AND emv.EmailAddress LIKE '%@%'
    WHERE pea.DeletedStatus = False
      AND pea.ActiveStatus = True
) AS ranked_email
WHERE rn = 1;

CREATE OR REPLACE TEMP VIEW vw_dim_apparatuspersonnel_final AS
SELECT DISTINCT
    b.tenant_id,
    b.ApparatusPersonnel_ID_Internal,
    b.Apparatus_ID_Internal,
    b.Incident_ID_Internal,
    LEFT(act.Apparatus_Personnel_Actions_Taken_1, 255) AS Apparatus_Personnel_Actions_Taken_1,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Category_1, 255) AS Apparatus_Personnel_Actions_Taken_Category_1,
    LEFT(act.Apparatus_Personnel_Actions_Taken_2, 255) AS Apparatus_Personnel_Actions_Taken_2,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Category_2, 255) AS Apparatus_Personnel_Actions_Taken_Category_2,
    LEFT(act.Apparatus_Personnel_Actions_Taken_3, 255) AS Apparatus_Personnel_Actions_Taken_3,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Category_3, 255) AS Apparatus_Personnel_Actions_Taken_Category_3,
    LEFT(act.Apparatus_Personnel_Actions_Taken_4, 255) AS Apparatus_Personnel_Actions_Taken_4,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Category_4, 255) AS Apparatus_Personnel_Actions_Taken_Category_4,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_1, 255) AS Apparatus_Personnel_Actions_Taken_Code_1,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_2, 255) AS Apparatus_Personnel_Actions_Taken_Code_2,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_3, 255) AS Apparatus_Personnel_Actions_Taken_Code_3,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_4, 255) AS Apparatus_Personnel_Actions_Taken_Code_4,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_And_Description_1, 255) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_1,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_And_Description_2, 255) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_2,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_And_Description_3, 255) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_3,
    LEFT(act.Apparatus_Personnel_Actions_Taken_Code_And_Description_4, 255) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_4,
    CAST(NULL AS STRING) AS Apparatus_Personnel_Actions_Taken_Code_And_Description_List,
    CAST(NULL AS STRING) AS Apparatus_Personnel_Actions_Taken_Code_List,
    CAST(NULL AS STRING) AS Apparatus_Personnel_Actions_Taken_List,
    LEFT(COALESCE(gr_att.DisplayMember, b.IsAttended), 255) AS Apparatus_Personnel_Attended_Flag,
    LEFT(b.LicenseNumber, 255) AS Apparatus_Personnel_ID,
    LEFT(COALESCE(ln.LastName, b.LastName), 255) AS Apparatus_Personnel_Last_Name,
    LEFT(gr_level.DisplayMember, 255) AS Apparatus_Personnel_Level,
    LEFT(COALESCE(fn.FirstName, b.FirstName), 255) AS Apparatus_Personnel_First_Name,
    LEFT(NULLIF(TRIM(CONCAT(COALESCE(fn.FirstName, b.FirstName, ''), ' ', COALESCE(ln.LastName, b.LastName, ''))), ''), 255) AS Apparatus_Personnel_Full_Name,
    LEFT(b.Rank, 255) AS Apparatus_Personnel_Rank_Or_Grade,
    CAST(b.Rate AS DECIMAL(18, 2)) AS Apparatus_Personnel_Rate,
    LEFT(COALESCE(gr_role.DisplayMember, role.PlusOneCode, role.Role), 255) AS Apparatus_Personnel_Role,
    b.TimeIn AS Apparatus_Personnel_Time_In_Date_Time,
    b.TimeOut AS Apparatus_Personnel_Time_Out_Date_Time,
    TRY_CAST(b.TimeSpent AS FLOAT) AS Apparatus_Personnel_Time_Spent,
    3 AS System_ID,
    current_timestamp() AS CreatedOn,
    current_timestamp() AS ModifiedOn,
    LEFT(COALESCE(gr_emp.DisplayMember, nv_emp.DisplayMember, emp_mv.EmploymentStatus, emp_mv.NotValue), 255) AS Apparatus_Personnel_Employment_Status,
    LEFT(agp.Position, 50) AS Apparatus_Personnel_Position,
    LEFT(agp.BadgeNumber, 50) AS Apparatus_Personnel_Badge_Number,
    CAST(NULL AS STRING) AS Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_HHMMSS,
    CAST(NULL AS INT) AS Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Seconds,
    CAST(NULL AS INT) AS Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Minutes,
    CASE WHEN emp_mv.EmploymentStatus = 'EMSPersonnelEmploymentStatus_FullTimePaidEmployee' THEN True ELSE False END AS Apparatus_Personnel_Is_Full_Time_Paid_Employee,
    CASE WHEN emp_mv.EmploymentStatus = 'EMSPersonnelEmploymentStatus_PartTimePaidEmployee' THEN True ELSE False END AS Apparatus_Personnel_Is_Part_Time_Paid_Employee,
    CASE WHEN emp_mv.EmploymentStatus = 'EMSPersonnelEmploymentStatus_NeitheranEmployeeNoraVolunteer' THEN True ELSE False END AS Apparatus_Personnel_Is_Neither_an_Employee_Nor_a_Volunteer,
    CASE WHEN emp_mv.EmploymentStatus = 'EMSPersonnelEmploymentStatus_Volunteer' THEN True ELSE False END AS Apparatus_Personnel_Is_Volunteer,
    CASE
        WHEN emp_mv.EmploymentStatus IN (
            'EMSPersonnelEmploymentStatus_FullTimePaidEmployee',
            'EMSPersonnelEmploymentStatus_PartTimePaidEmployee'
        ) THEN True
        ELSE False
    END AS Apparatus_Personnel_Is_Career,
    LEFT(COALESCE(gr_app.DisplayMember, nv_app.DisplayMember, at.Type, at.NotValue), 255) AS Apparatus_Personnel_Apparatus_Type,
    CASE
        WHEN lower(gr_app.DisplayMember) LIKE '%air%'
          OR lower(gr_app.DisplayMember) LIKE '%aerial%'
          OR lower(gr_app.DisplayMember) LIKE '%heli%' THEN False
        WHEN lower(gr_app.DisplayMember) LIKE '%truck%'
          OR lower(gr_app.DisplayMember) LIKE '%car%'
          OR lower(gr_app.DisplayMember) LIKE '%engine%'
          OR lower(gr_app.DisplayMember) LIKE '%vehicle%'
          OR lower(gr_app.DisplayMember) LIKE '%tanker%'
          OR lower(gr_app.DisplayMember) LIKE '%dozer%'
          OR lower(gr_app.DisplayMember) LIKE '%tractor%' THEN True
        ELSE False
    END AS Apparatus_Personnel_Is_In_Engine_Apparatus,
    CASE
        WHEN lower(gr_app.DisplayMember) LIKE '%air%'
          OR lower(gr_app.DisplayMember) LIKE '%aerial%'
          OR lower(gr_app.DisplayMember) LIKE '%heli%' THEN True
        ELSE False
    END AS Apparatus_Personnel_Is_In_Aerial_Apparatus,
    CASE
        WHEN lower(gr_app.DisplayMember) LIKE '%air%'
          OR lower(gr_app.DisplayMember) LIKE '%aerial%'
          OR lower(gr_app.DisplayMember) LIKE '%heli%'
          OR lower(gr_app.DisplayMember) LIKE '%truck%'
          OR lower(gr_app.DisplayMember) LIKE '%car%'
          OR lower(gr_app.DisplayMember) LIKE '%engine%'
          OR lower(gr_app.DisplayMember) LIKE '%vehicle%'
          OR lower(gr_app.DisplayMember) LIKE '%tanker%'
          OR lower(gr_app.DisplayMember) LIKE '%dozer%'
          OR lower(gr_app.DisplayMember) LIKE '%tractor%' THEN False
        WHEN NULLIF(COALESCE(at.Type, at.NotValue), '') IS NOT NULL THEN True
        ELSE False
    END AS Apparatus_Personnel_Is_In_Other_Apparatus,
    LEFT(COALESCE(pr.Description, b.PayRateDescription), 255) AS Apparatus_Personnel_Pay_Rate_Description,
    TRY_CAST(b.TimeSpent AS DECIMAL(18, 2)) AS Apparatus_Personnel_Hours_Spent,
    LEFT(email.EmailAddress, 255) AS Apparatus_Personnel_Email_Address,
    current_date() AS _ingest_date,
    ${pipeline_run_id} AS batch_id
FROM <source_silver_schema>.stg_dim_apparatuspersonnel_base AS b
LEFT JOIN <source_bronze_schema>.fireevent_apparatustypemodvalue AS at
    ON b.tenant_id = at.tenant_id
   AND b.Apparatus_ID_Internal = at.ApparatusID
   AND at.DeletedStatus = False
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr_app
    ON at.tenant_id = gr_app.tenant_id
   AND at.Type = gr_app.GlobalResourceID
   AND gr_app.GlobalResourceGroupID = 'ApparatusorResourceType'
LEFT JOIN vw_dim_apparatuspersonnel_nv_lookup AS nv_app
    ON at.tenant_id = nv_app.tenant_id
   AND at.NotValue = nv_app.GlobalResourceID
   AND nv_app.GlobalResourceGroupID = 'ApparatusorResourceType'
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr_att
    ON b.tenant_id = gr_att.tenant_id
   AND b.IsAttended = gr_att.GlobalResourceID
   AND gr_att.GlobalResourceGroupID = 'PersonnelAttended'
LEFT JOIN <source_bronze_schema>.fireevent_apparatuspersonnellevelmodvalue AS lvl
    ON b.tenant_id = lvl.tenant_id
   AND b.ApparatusPersonnel_ID_Internal = lvl.ApparatusPersonnelID
   AND lvl.DeletedStatus = False
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr_level
    ON lvl.tenant_id = gr_level.tenant_id
   AND COALESCE(lvl.PlusOneCode, lvl.Level) = gr_level.GlobalResourceID
   AND gr_level.GlobalResourceGroupID = 'PersonnelLevel'
LEFT JOIN <common_bronze_schema>.resource_payrate AS pr
    ON b.tenant_id = pr.tenant_id
   AND b.PayRateID = pr.PayRateID
   AND pr.DeletedStatus = False
LEFT JOIN <common_bronze_schema>.resource_performerfirstnamemodvalue AS fn
    ON b.tenant_id = fn.tenant_id
   AND b.PerformerID = fn.PerformerID
LEFT JOIN <common_bronze_schema>.resource_performerlastnamemodvalue AS ln
    ON b.tenant_id = ln.tenant_id
   AND b.PerformerID = ln.PerformerID
LEFT JOIN <common_bronze_schema>.resource_agencyperformer AS agp
    ON b.tenant_id = agp.tenant_id
   AND b.AgencyID = agp.AgencyID
   AND b.PerformerID = agp.PerformerID
   AND agp.DeletedStatus = False
LEFT JOIN <common_bronze_schema>.resource_agencyperformeremployment AS emp
    ON agp.tenant_id = emp.tenant_id
   AND agp.AgencyID = emp.AgencyID
   AND agp.PerformerID = emp.PerformerID
   AND emp.DeletedStatus = False
LEFT JOIN <common_bronze_schema>.resource_agencyperformeremploymentemploymentstatusmodvalue AS emp_mv
    ON emp.tenant_id = emp_mv.tenant_id
   AND emp.AgencyID = emp_mv.AgencyID
   AND emp.PerformerID = emp_mv.PerformerID
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr_emp
    ON emp_mv.tenant_id = gr_emp.tenant_id
   AND emp_mv.EmploymentStatus = gr_emp.GlobalResourceID
   AND gr_emp.GlobalResourceGroupID = 'EMSPersonnelEmploymentStatus'
LEFT JOIN vw_dim_apparatuspersonnel_nv_lookup AS nv_emp
    ON emp_mv.tenant_id = nv_emp.tenant_id
   AND emp_mv.NotValue = nv_emp.GlobalResourceID
   AND nv_emp.GlobalResourceGroupID = 'EMSPersonnelEmploymentStatus'
LEFT JOIN vw_dim_apparatuspersonnel_primary_email AS email
    ON b.tenant_id = email.tenant_id
   AND b.PerformerID = email.PerformerID
LEFT JOIN <source_bronze_schema>.fireevent_apparatuspersonnelrolemodvalue AS role
    ON b.tenant_id = role.tenant_id
   AND b.ApparatusPersonnel_ID_Internal = role.ApparatusPersonnelID
LEFT JOIN <source_silver_schema>.stg_dim_apparatuspersonnel_gr_lookup AS gr_role
    ON role.tenant_id = gr_role.tenant_id
   AND COALESCE(role.PlusOneCode, role.Role) = gr_role.GlobalResourceID
   AND gr_role.GlobalResourceGroupID = 'PersonnelRole'
LEFT JOIN vw_dim_apparatuspersonnel_actions_pivot AS act
    ON b.tenant_id = act.tenant_id
   AND b.ApparatusPersonnel_ID_Internal = act.ApparatusPersonnel_ID_Internal;

MERGE INTO <source_silver_schema>.Dim_ApparatusPersonnel AS target
USING vw_dim_apparatuspersonnel_final AS source
ON (
    target.tenant_id = source.tenant_id
    AND target.ApparatusPersonnel_ID_Internal = source.ApparatusPersonnel_ID_Internal
    AND target.Apparatus_ID_Internal = source.Apparatus_ID_Internal
    AND target.Incident_ID_Internal = source.Incident_ID_Internal
)
WHEN MATCHED THEN UPDATE SET
    target.Apparatus_Personnel_Actions_Taken_1 = source.Apparatus_Personnel_Actions_Taken_1,
    target.Apparatus_Personnel_Actions_Taken_Category_1 = source.Apparatus_Personnel_Actions_Taken_Category_1,
    target.Apparatus_Personnel_Actions_Taken_2 = source.Apparatus_Personnel_Actions_Taken_2,
    target.Apparatus_Personnel_Actions_Taken_Category_2 = source.Apparatus_Personnel_Actions_Taken_Category_2,
    target.Apparatus_Personnel_Actions_Taken_3 = source.Apparatus_Personnel_Actions_Taken_3,
    target.Apparatus_Personnel_Actions_Taken_Category_3 = source.Apparatus_Personnel_Actions_Taken_Category_3,
    target.Apparatus_Personnel_Actions_Taken_4 = source.Apparatus_Personnel_Actions_Taken_4,
    target.Apparatus_Personnel_Actions_Taken_Category_4 = source.Apparatus_Personnel_Actions_Taken_Category_4,
    target.Apparatus_Personnel_Actions_Taken_Code_1 = source.Apparatus_Personnel_Actions_Taken_Code_1,
    target.Apparatus_Personnel_Actions_Taken_Code_2 = source.Apparatus_Personnel_Actions_Taken_Code_2,
    target.Apparatus_Personnel_Actions_Taken_Code_3 = source.Apparatus_Personnel_Actions_Taken_Code_3,
    target.Apparatus_Personnel_Actions_Taken_Code_4 = source.Apparatus_Personnel_Actions_Taken_Code_4,
    target.Apparatus_Personnel_Actions_Taken_Code_And_Description_1 = source.Apparatus_Personnel_Actions_Taken_Code_And_Description_1,
    target.Apparatus_Personnel_Actions_Taken_Code_And_Description_2 = source.Apparatus_Personnel_Actions_Taken_Code_And_Description_2,
    target.Apparatus_Personnel_Actions_Taken_Code_And_Description_3 = source.Apparatus_Personnel_Actions_Taken_Code_And_Description_3,
    target.Apparatus_Personnel_Actions_Taken_Code_And_Description_4 = source.Apparatus_Personnel_Actions_Taken_Code_And_Description_4,
    target.Apparatus_Personnel_Attended_Flag = source.Apparatus_Personnel_Attended_Flag,
    target.Apparatus_Personnel_ID = source.Apparatus_Personnel_ID,
    target.Apparatus_Personnel_Last_Name = source.Apparatus_Personnel_Last_Name,
    target.Apparatus_Personnel_Level = source.Apparatus_Personnel_Level,
    target.Apparatus_Personnel_First_Name = source.Apparatus_Personnel_First_Name,
    target.Apparatus_Personnel_Full_Name = source.Apparatus_Personnel_Full_Name,
    target.Apparatus_Personnel_Rank_Or_Grade = source.Apparatus_Personnel_Rank_Or_Grade,
    target.Apparatus_Personnel_Rate = source.Apparatus_Personnel_Rate,
    target.Apparatus_Personnel_Role = source.Apparatus_Personnel_Role,
    target.Apparatus_Personnel_Time_In_Date_Time = source.Apparatus_Personnel_Time_In_Date_Time,
    target.Apparatus_Personnel_Time_Out_Date_Time = source.Apparatus_Personnel_Time_Out_Date_Time,
    target.Apparatus_Personnel_Time_Spent = source.Apparatus_Personnel_Time_Spent,
    target.System_ID = source.System_ID,
    target.ModifiedOn = source.ModifiedOn,
    target.Apparatus_Personnel_Employment_Status = source.Apparatus_Personnel_Employment_Status,
    target.Apparatus_Personnel_Position = source.Apparatus_Personnel_Position,
    target.Apparatus_Personnel_Badge_Number = source.Apparatus_Personnel_Badge_Number,
    target.Apparatus_Personnel_Is_Full_Time_Paid_Employee = source.Apparatus_Personnel_Is_Full_Time_Paid_Employee,
    target.Apparatus_Personnel_Is_Part_Time_Paid_Employee = source.Apparatus_Personnel_Is_Part_Time_Paid_Employee,
    target.Apparatus_Personnel_Is_Neither_an_Employee_Nor_a_Volunteer = source.Apparatus_Personnel_Is_Neither_an_Employee_Nor_a_Volunteer,
    target.Apparatus_Personnel_Is_Volunteer = source.Apparatus_Personnel_Is_Volunteer,
    target.Apparatus_Personnel_Is_Career = source.Apparatus_Personnel_Is_Career,
    target.Apparatus_Personnel_Apparatus_Type = source.Apparatus_Personnel_Apparatus_Type,
    target.Apparatus_Personnel_Is_In_Engine_Apparatus = source.Apparatus_Personnel_Is_In_Engine_Apparatus,
    target.Apparatus_Personnel_Is_In_Aerial_Apparatus = source.Apparatus_Personnel_Is_In_Aerial_Apparatus,
    target.Apparatus_Personnel_Is_In_Other_Apparatus = source.Apparatus_Personnel_Is_In_Other_Apparatus,
    target.Apparatus_Personnel_Pay_Rate_Description = source.Apparatus_Personnel_Pay_Rate_Description,
    target.Apparatus_Personnel_Hours_Spent = source.Apparatus_Personnel_Hours_Spent,
    target.Apparatus_Personnel_Email_Address = source.Apparatus_Personnel_Email_Address,
    target._ingest_date = source._ingest_date,
    target.batch_id = source.batch_id
WHEN NOT MATCHED THEN INSERT (
    ApparatusPersonnel_ID_Internal,
    Apparatus_ID_Internal,
    Incident_ID_Internal,
    Apparatus_Personnel_Actions_Taken_1,
    Apparatus_Personnel_Actions_Taken_Category_1,
    Apparatus_Personnel_Actions_Taken_2,
    Apparatus_Personnel_Actions_Taken_Category_2,
    Apparatus_Personnel_Actions_Taken_3,
    Apparatus_Personnel_Actions_Taken_Category_3,
    Apparatus_Personnel_Actions_Taken_4,
    Apparatus_Personnel_Actions_Taken_Category_4,
    Apparatus_Personnel_Actions_Taken_Code_1,
    Apparatus_Personnel_Actions_Taken_Code_2,
    Apparatus_Personnel_Actions_Taken_Code_3,
    Apparatus_Personnel_Actions_Taken_Code_4,
    Apparatus_Personnel_Actions_Taken_Code_And_Description_1,
    Apparatus_Personnel_Actions_Taken_Code_And_Description_2,
    Apparatus_Personnel_Actions_Taken_Code_And_Description_3,
    Apparatus_Personnel_Actions_Taken_Code_And_Description_4,
    Apparatus_Personnel_Actions_Taken_Code_And_Description_List,
    Apparatus_Personnel_Actions_Taken_Code_List,
    Apparatus_Personnel_Actions_Taken_List,
    Apparatus_Personnel_Attended_Flag,
    Apparatus_Personnel_ID,
    Apparatus_Personnel_Last_Name,
    Apparatus_Personnel_Level,
    Apparatus_Personnel_First_Name,
    Apparatus_Personnel_Full_Name,
    Apparatus_Personnel_Rank_Or_Grade,
    Apparatus_Personnel_Rate,
    Apparatus_Personnel_Role,
    Apparatus_Personnel_Time_In_Date_Time,
    Apparatus_Personnel_Time_Out_Date_Time,
    Apparatus_Personnel_Time_Spent,
    System_ID,
    CreatedOn,
    ModifiedOn,
    Apparatus_Personnel_Employment_Status,
    Apparatus_Personnel_Position,
    Apparatus_Personnel_Badge_Number,
    Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_HHMMSS,
    Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Seconds,
    Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Minutes,
    Apparatus_Personnel_Is_Full_Time_Paid_Employee,
    Apparatus_Personnel_Is_Part_Time_Paid_Employee,
    Apparatus_Personnel_Is_Neither_an_Employee_Nor_a_Volunteer,
    Apparatus_Personnel_Is_Volunteer,
    Apparatus_Personnel_Is_Career,
    Apparatus_Personnel_Apparatus_Type,
    Apparatus_Personnel_Is_In_Engine_Apparatus,
    Apparatus_Personnel_Is_In_Aerial_Apparatus,
    Apparatus_Personnel_Is_In_Other_Apparatus,
    Apparatus_Personnel_Pay_Rate_Description,
    Apparatus_Personnel_Hours_Spent,
    Apparatus_Personnel_Email_Address,
    tenant_id,
    _ingest_date,
    batch_id
)
VALUES (
    source.ApparatusPersonnel_ID_Internal,
    source.Apparatus_ID_Internal,
    source.Incident_ID_Internal,
    source.Apparatus_Personnel_Actions_Taken_1,
    source.Apparatus_Personnel_Actions_Taken_Category_1,
    source.Apparatus_Personnel_Actions_Taken_2,
    source.Apparatus_Personnel_Actions_Taken_Category_2,
    source.Apparatus_Personnel_Actions_Taken_3,
    source.Apparatus_Personnel_Actions_Taken_Category_3,
    source.Apparatus_Personnel_Actions_Taken_4,
    source.Apparatus_Personnel_Actions_Taken_Category_4,
    source.Apparatus_Personnel_Actions_Taken_Code_1,
    source.Apparatus_Personnel_Actions_Taken_Code_2,
    source.Apparatus_Personnel_Actions_Taken_Code_3,
    source.Apparatus_Personnel_Actions_Taken_Code_4,
    source.Apparatus_Personnel_Actions_Taken_Code_And_Description_1,
    source.Apparatus_Personnel_Actions_Taken_Code_And_Description_2,
    source.Apparatus_Personnel_Actions_Taken_Code_And_Description_3,
    source.Apparatus_Personnel_Actions_Taken_Code_And_Description_4,
    source.Apparatus_Personnel_Actions_Taken_Code_And_Description_List,
    source.Apparatus_Personnel_Actions_Taken_Code_List,
    source.Apparatus_Personnel_Actions_Taken_List,
    source.Apparatus_Personnel_Attended_Flag,
    source.Apparatus_Personnel_ID,
    source.Apparatus_Personnel_Last_Name,
    source.Apparatus_Personnel_Level,
    source.Apparatus_Personnel_First_Name,
    source.Apparatus_Personnel_Full_Name,
    source.Apparatus_Personnel_Rank_Or_Grade,
    source.Apparatus_Personnel_Rate,
    source.Apparatus_Personnel_Role,
    source.Apparatus_Personnel_Time_In_Date_Time,
    source.Apparatus_Personnel_Time_Out_Date_Time,
    source.Apparatus_Personnel_Time_Spent,
    source.System_ID,
    source.CreatedOn,
    source.ModifiedOn,
    source.Apparatus_Personnel_Employment_Status,
    source.Apparatus_Personnel_Position,
    source.Apparatus_Personnel_Badge_Number,
    source.Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_HHMMSS,
    source.Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Seconds,
    source.Apparatus_Personnel_Time_In_To_Personnel_Time_Out_In_Minutes,
    source.Apparatus_Personnel_Is_Full_Time_Paid_Employee,
    source.Apparatus_Personnel_Is_Part_Time_Paid_Employee,
    source.Apparatus_Personnel_Is_Neither_an_Employee_Nor_a_Volunteer,
    source.Apparatus_Personnel_Is_Volunteer,
    source.Apparatus_Personnel_Is_Career,
    source.Apparatus_Personnel_Apparatus_Type,
    source.Apparatus_Personnel_Is_In_Engine_Apparatus,
    source.Apparatus_Personnel_Is_In_Aerial_Apparatus,
    source.Apparatus_Personnel_Is_In_Other_Apparatus,
    source.Apparatus_Personnel_Pay_Rate_Description,
    source.Apparatus_Personnel_Hours_Spent,
    source.Apparatus_Personnel_Email_Address,
    source.tenant_id,
    source._ingest_date,
    source.batch_id
);
