// Node.js ES Module script to generate SQLX files for Dataform without SCD Type 2 logic
 
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
 
// Generate SQLX for Hub, Link, Satellite (no SCD Type 2)
function generateSQLX(row) {
  const { table_name, table_type, business_key, descriptive_fields, source_table } = row;
 
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
  '${source_table}' AS record_source
FROM
  \${ref("${source_table}")}
`;
  }
 
  else if (table_type === "link") {
    const linkKeys = business_key.split("|").map(k => k.trim());
    sql = `
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
 
  else if (table_type === "satellite") {
    sql = `
config {
  type: "table",
  tags: ["satellite"],
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
