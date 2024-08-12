const fs = require("fs");
const csv = require("csv-parser");
const { parse } = require("json2csv");

// Function to parse CSV files
function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ";" })) // Adjusted separator to semicolon
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", (err) => reject(err));
  });
}

// Function to load data from CSV files
async function loadData() {
  const apoData = await parseCSV("./data/bnu-lt-data.csv");
  const matches = await parseCSV("./data/matches.csv");

  console.log("check before", apoData);

  return { apoData, matches };
}

// Function to map related manufacturers
function mapRelatedManufacturers(matches, apoData) {
  const manufacturerMap = {};

  matches.forEach((match) => {
    const mainProduct = apoData.find((p) => {
      // console.log("first", p, match.m_source_id);
      return p.source_id === match.m_source_id && p.source === match.m_source;
    });
    const compProduct = apoData.find(
      (p) => p.source_id === match.c_source_id && p.source === match.c_source
    );

    // console.log("mainProduct", mainProduct);
    // console.log("compProduct", compProduct);

    if (mainProduct && compProduct) {
      const mainManufacturer = mainProduct.manufacturer;
      const compManufacturer = compProduct.manufacturer;

      if (mainManufacturer && compManufacturer) {
        if (!manufacturerMap[mainManufacturer]) {
          manufacturerMap[mainManufacturer] = new Set();
        }
        manufacturerMap[mainManufacturer].add(compManufacturer);

        // Ensure the reverse relationship is also recorded
        if (!manufacturerMap[compManufacturer]) {
          manufacturerMap[compManufacturer] = new Set();
        }
        manufacturerMap[compManufacturer].add(mainManufacturer);
      }
    }
  });

  return manufacturerMap;
}

// Function to assign relation types
function assignRelationType(manufacturerMap) {
  const relations = [];

  Object.keys(manufacturerMap).forEach((parent) => {
    manufacturerMap[parent].forEach((child) => {
      relations.push({ parent, child, relation_type: "sibling" });
    });
  });

  return relations;
}

// Function to save mapped data to CSV
function saveToCSV(manufacturerRelations) {
  if (manufacturerRelations.length === 0) {
    console.error("No manufacturer relations found to save.");
    return;
  }

  const fields = ["parent", "child", "relation_type"];
  const csvData = parse(manufacturerRelations, { fields });

  fs.writeFileSync("./output/manufacturerRelations.csv", csvData);
  console.log("CSV file has been saved.");
}

// Function to assign a manufacturer based on the title
function assignManufacturerByTitle(title, apoData, manufacturerMap) {
  const product = apoData.find((p) => p.title === title);
  if (product) {
    const relatedManufacturers = manufacturerMap[product.manufacturer];
    if (relatedManufacturers && relatedManufacturers.size > 0) {
      return Array.from(relatedManufacturers)[0]; // Return one of the related manufacturers
    }
  }
  return null;
}

// Main function to execute the process
async function main() {
  const { apoData, matches } = await loadData();

  // console.log("check matches", matches.length);
  // console.log("check matches", apoData.length);

  const manufacturerMap = mapRelatedManufacturers(matches, apoData);

  console.log("check mmm", manufacturerMap);

  const manufacturerRelations = assignRelationType(manufacturerMap);

  saveToCSV(manufacturerRelations);

  // Example of assigning a manufacturer by title
  const exampleTitle = "Your Product Title Here";
  const assignedManufacturer = assignManufacturerByTitle(
    exampleTitle,
    apoData,
    manufacturerMap
  );
  console.log(
    `Assigned Manufacturer for "${exampleTitle}":`,
    assignedManufacturer
  );
}

main();
