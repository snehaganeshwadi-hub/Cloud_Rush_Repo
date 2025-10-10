// auto_generator.mjs
// Node.js ES Module script to generate SQLX files for Dataform with SCD Type 2 logic
import fs from "fs";
import path from "path";
import readline from "readline";
// Metadata CSV path
const metadataFile = "./metadata.csv";
const outputDir = "./definitions/auto_generated"; // SQLX output directory
// Ensure output directory exists
if (!fs.existsSync(outputDir)) {
 fs.mkdirSync(outputDir);
}
// Function to parse CSV
async function parseCSV(filePath) {
 const fileStream = fs.createReadStream(filePath);
 const rl = readline.createInterface({
   input: fileStream,
   crlfDelay: Infinity
 });
 const rows = [];
 let headers = [];
 for await (const line of rl) {
   if (!headers.length) {
     headers = line.split(",");
   } else {
     const values = line.split(",");
     const row = {};
     headers.forEach((h, i) => {
       row[h.trim()] = values[i] ? values[i].trim() : "";
     });
     rows.push(row);
   }
 }
 return rows;
}
// Generate SQLX for Hub, Link, Satellite with SCD Type 2
function generateSQLX(row) {
 const { table_name, table_type, business_key, descriptive_fields, source_table } = row;
  // Split composite keys (comma-separated)
 const keys = business_key.split("_").map(k => k.trim());
 const key_select = keys.join("_ ");
 const key_join = keys.map(k => `s.${k} = t.${k}`).join(" and ");
 let sql = "";
 if (table_type === "hub") {
   sql = `
config {
 type: "table",
  tags: ["hub"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  CURRENT_DATE() AS load_dts,
  '${source_table}' as record_source
FROM
  \${ref("${source_table}")}
   `;
 }
 
else if (table_type === "link") {
  const linkKeys = business_key.split("|").map(k => k.trim());
  const concatExpr = `CONCAT(${linkKeys.map(k => `${k}`).join(", ")})`;
  const hkName = `${linkKeys.join("_")}_hk`;
  const bkFields = linkKeys.map(k => `${k} AS ${k}_bk`).join(",\n  ");	 
   sql = `
config {
 type: "table",
  tags: ["link"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: [${linkKeys.map(k => `"${k}_bk"`).join(", ")}]
}

SELECT
  TO_HEX(SHA256(CAST(CONCAT(${business_key.split("|").map(k => `l.${k.trim()}`).join(", ")}) AS STRING))) AS ${business_key.split("|").map(k => k.trim()).join("_")}_hk,
  ${business_key.split("|").map(k => `CAST(l.${k.trim()} AS STRING) AS ${k.trim()}_bk`).join(",\n  ")},
  CURRENT_DATE() AS load_dts,
  '${source_table}' AS record_source
FROM
  \${ref("${source_table}")} AS l
`;
 }
else if (table_type === "satellite") {
  const baseTableName = `${table_name}_base`;
  const mergeTableName = `${table_name}_merge`;
 
  const baseSQL = `
config {
  type: "table",
  tags: ["satellite", "base"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
 
SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  ${descriptive_fields.replace(/\|/g, ",")},
  CURRENT_DATE() AS load_dts,
  '${source_table}' AS record_source
FROM
  \${ref("${source_table}")}
`;
 
  const mergeSQL = `
config {
  type: "operations",
  tags: ["scd2", "satellite"],
  description: "SCD Type 2 update for ${table_name}"
}
 
MERGE INTO \${ref("${table_name}")} AS target
USING (
  SELECT
    TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
    ${business_key} AS ${business_key}_bk,
    ${descriptive_fields.replace(/\|/g, ",")},
    CURRENT_DATE() AS load_dts,
    '${source_table}' AS record_source
  FROM \${ref("${source_table}")}
) AS source
ON target.${business_key}_bk = source.${business_key}_bk AND target.is_current = TRUE
 
WHEN MATCHED AND (
  ${descriptive_fields.split("|").map(f => `source.${f} <> target.${f}`).join(" OR ")}
) THEN
  UPDATE SET
    is_current = FALSE,
    effective_end_date = source.load_dts
 
WHEN NOT MATCHED THEN
  INSERT (
    ${business_key}_hk,
    ${business_key}_bk,
    ${descriptive_fields.replace(/\|/g, ",")},
    load_dts,
    record_source,
    is_current,
    effective_start_date,
    effective_end_date
  )
  VALUES (
    source.${business_key}_hk,
    source.${business_key}_bk,
    ${descriptive_fields.split("|").map(f => `source.${f}`).join(", ")},
    source.load_dts,
    source.record_source,
    TRUE,
    source.load_dts,
    NULL
  )
`;
 
  return { baseSQL, mergeSQL };
}	
// Main execution
(async () => {
 const rows = await parseCSV(metadataFile);
 rows.forEach(row => {
   const fileName = `${row.table_name}.sqlx`;
   const sqlxContent = generateSQLX(row);
   fs.writeFileSync(path.join(outputDir, fileName), sqlxContent);
   console.log(`Generated: ${fileName}`);

  if (row.table_type === "satellite") {
  const { baseSQL, mergeSQL } = generateSQLX(row);
  fs.writeFileSync(path.join(outputDir, `${row.table_name}_base.sqlx`), baseSQL);
  fs.writeFileSync(path.join(outputDir, `${row.table_name}_merge.sqlx`), mergeSQL);}	 
 });
})();
