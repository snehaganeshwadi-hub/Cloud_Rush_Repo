// Node.js ES Module script to generate SQLX files for Dataform with SCD Type 2 logic
 
import fs from "fs";
import path from "path";
import readline from "readline";
 
// Metadata CSV path
const metadataFile = "./metadata.csv";
 
// SQLX output directory
const outputDir = "./definitions/auto_generated";
 
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
 
  if (table_type === "hub") {
    return `
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
  '${source_table}' AS record_source
FROM
  \${ref("${source_table}")}
`;
  }
 
  if (table_type === "link") {
    const linkKeys = business_key.split("|").map(k => k.trim());
    return `
config {
  type: "table",
  tags: ["link"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: [${linkKeys.map(k => `"${k}_bk"`).join(", ")}]
}
 
SELECT
  TO_HEX(SHA256(CAST(CONCAT(${linkKeys.map(k => `l.${k}`).join(", ")}) AS STRING))) AS ${linkKeys.join("_")}_hk,
  ${linkKeys.map(k => `CAST(l.${k} AS STRING) AS ${k}_bk`).join(",\n  ")},
  CURRENT_DATE() AS load_dts,
  '${source_table}' AS record_source
FROM
  \${ref("${source_table}")} AS l
`;
  }
 
  if (table_type === "satellite") {
    const baseSQL = `
config {
  name: "${table_name}_base",
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
  name: "${table_name}_merge",
  type: "operations",
  tags: ["scd2", "satellite"],
  description: "SCD Type 2 update for ${table_name}" } 

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
 
  return "";
}
 
// Main execution
(async () => {
  const rows = await parseCSV(metadataFile);
 
  rows.forEach(row => {
    if (row.table_type === "satellite") {
      const { baseSQL, mergeSQL } = generateSQLX(row);
 
      fs.writeFileSync(path.join(outputDir, `${row.table_name}_base.sqlx`), baseSQL);
      fs.writeFileSync(path.join(outputDir, `${row.table_name}_merge.sqlx`), mergeSQL);
 
      console.log(`Generated: ${row.table_name}_base.sqlx and ${row.table_name}_merge.sqlx`);
    } else {
      const sqlxContent = generateSQLX(row); // returns a string for hub/link
      fs.writeFileSync(path.join(outputDir, `${row.table_name}.sqlx`), sqlxContent);
 
      console.log(`Generated: ${row.table_name}.sqlx`);
    }
  });
})();
