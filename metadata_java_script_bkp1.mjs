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
   sql = `
config {
 type: "table",
  tags: ["satellite"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
with source_data as (
 SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  CURRENT_DATE() AS load_dts,
  '${source_table}' as record_source
FROM
  \${ref("${source_table}")}
),
scd2 as (
 select
   s.${business_key}_bk,
   s.${descriptive_fields.replace(/\|/g, ", s.")},
   s.load_dts,
   s.record_source,
   case
     when t.${business_key}_bk is null then true
     when ${descriptive_fields.split("|").map(f => `s.${f} <> t.${f}`).join(" or ")} then true
     else false
   end as is_changed
 from source_data s
 left join cloud-rush-473406.Row_Vault.${table_name} as t
 on s.${business_key}_bk = t.${business_key}_bk
 and t.is_current = true
)
select
 ${business_key}_bk,
 ${descriptive_fields.replace(/\|/g, ",")},
 load_dts,
 record_source,
 case when is_changed then true else false end as is_current,
 case when is_changed then load_dts else null end as effective_start_date,
 null as effective_end_date
from scd2
   `;
 }
 return sql;
}
// Main execution
(async () => {
 const rows = await parseCSV(metadataFile);
 rows.forEach(row => {
   const fileName = `${row.table_name}.sqlx`;
   const sqlxContent = generateSQLX(row);
   fs.writeFileSync(path.join(outputDir, fileName), sqlxContent);
   console.log(`Generated: ${fileName}`);
 });
})();
