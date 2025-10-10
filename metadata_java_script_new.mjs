// auto_generator.mjs
// Generates SQLX files for Dataform with separate logic for base table creation and SCD Type 2 tracking
import fs from "fs";
import path from "path";
import readline from "readline";
// Metadata CSV path
const metadataFile = "./metadata.csv";
const outputDir = "./definitions/auto_gen_sqlx"; // SQLX output directory
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
// Function to generate SQL for hub, link, and satellite
function generateSQLX(row) {
 const { table_name, table_type, business_key, descriptive_fields, source_table } = row;
 let sqlFiles = [];
 if (table_type === "hub") {
   const hubSQL = `
config {
 type: "table",
  tags: ["hub"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
select distinct
 ${business_key} as business_key,
 current_timestamp() as load_dts,
 '${source_table}' as record_source
from \${ref("${source_table}")}
   `;
   sqlFiles.push({ name: `${table_name}.sqlx`, content: hubSQL });
 }
 else if (table_type === "link") {
   const linkSQL = `
config {
 type: "table",
  tags: ["link"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
select distinct
 ${business_key} as business_key,
 current_timestamp() as load_dts,
 '${source_table}' as record_source
from \${ref("${source_table}")}
   `;
   sqlFiles.push({ name: `${table_name}.sqlx`, content: linkSQL });
 }
 else if (table_type === "satellite") {
   const desc_fields_arr = descriptive_fields.split(",").map(f => f.trim());
   const desc_field_list = desc_fields_arr.join(", ");
   const compare_conditions = desc_fields_arr.map(f => `s.${f} <> t.${f}`).join(" or ");
   // ---- Base satellite table (initial load) ----
   const baseSQL = `
config {
 type: "table",
  tags: ["satellite"],
  schema: "Row_Vault",
  partitionBy: "load_dts",
  clusterBy: ["${business_key}_bk"]
}
// Base Satellite Load (Initial)
select
 ${business_key} as ${business_key}_hk ,
 ${desc_field_list},
 current_timestamp() as load_dts,
 '${source_table}' as record_source,
 true as is_current,
 current_timestamp() as effective_start_date,
 null as effective_end_date
from \${ref("${source_table}")}
   `;
   // ---- SCD Type 2 table update ----
   const scd2SQL = `
config {
 type: "table"
}
// SCD Type 2 Tracking
// Compare current vs previous snapshot and track changes
with source_data as (
 select
   ${business_key} as business_key,
   ${desc_field_list},
   current_timestamp() as load_dts,
   '${source_table}' as record_source
 from \${ref("${source_table}")}
),
scd2 as (
 select
   s.business_key,
   ${desc_field_list.split(",").map(f => `s.${f.trim()}`).join(", ")},
   s.load_dts,
   s.record_source,
   case
     when t.business_key is null then true
     when ${compare_conditions} then true
     else false
   end as is_changed
 from source_data s
 left join ${table_name}_base t
 on s.business_key = t.business_key
 and t.is_current = true
)
select
 business_key,
 ${desc_field_list},
 load_dts,
 record_source,
 is_changed as is_current,
 case when is_changed then load_dts else null end as effective_start_date,
 null as effective_end_date
from scd2;
   `;
   sqlFiles.push({ name: `${table_name}_base.sqlx`, content: baseSQL });
   sqlFiles.push({ name: `${table_name}_scd2.sqlx`, content: scd2SQL });
 }
 return sqlFiles;
}
// ---- Main Execution ----
(async () => {
 const rows = await parseCSV(metadataFile);
 rows.forEach(row => {
   const sqlFiles = generateSQLX(row);
   sqlFiles.forEach(file => {
     fs.writeFileSync(path.join(outputDir, file.name), file.content);
     console.log(`✅ Generated: ${file.name}`);
   });
 });
})();
