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
 let sql = "";
 if (table_type === "hub") {
   sql = `
config {
 type: "table",
  tags: ["hub"],
  partitionBy: "load_dts",
  clusterBy: ["${business_key}"]
}
SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  CURRENT_DATE() AS load_dts,
  '${source_table}' as record_source
FROM
  \${ref('${source_table}')}
   `;
 }
 else if (table_type === "link") {
   sql = `
config {
 type: "table",
  tags: ["hub"],
  partitionBy: "load_dts",
  clusterBy: ["${business_key}"]
}
SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  CURRENT_DATE() AS load_dts,
  '${source_table}' as record_source
FROM
  \${ref('${source_table}')}
`;
 }
 else if (table_type === "satellite") {
   sql = `
config {
 type: "table",
  tags: ["hub"],
  partitionBy: "load_dts",
  clusterBy: ["${business_key}"]
}
// SCD Type 2 History Tracking
// Track changes in descriptive attributes
with source_data as (
 SELECT
  TO_HEX(SHA256(CAST(${business_key} AS STRING))) AS ${business_key}_hk,
  ${business_key} AS ${business_key}_bk,
  CURRENT_DATE() AS load_dts,
  '${source_table}' as record_source
FROM
  \${ref('${source_table}')}
),
scd2 as (
 select
   s.${business_key},
   s.${descriptive_fields.replace(/\|/g, ", s.")},
   s.load_dts,
   s.record_source,
   case
     when t.${business_key} is null then true
     when ${descriptive_fields.split("|").map(f => `s.${f} <> t.${f}`).join(" or ")} then true
     else false
   end as is_changed
 from source_data s
 left join ${table_name} t
 on s.${business_key} = t.${business_key}
 and t.is_current = true
)
select
 ${business_key},
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
