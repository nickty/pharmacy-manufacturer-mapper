const fs = require('fs');
const csv = require('csv-parser');
const { parse } = require('json2csv');

// Function to parse CSV files
function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv()) // Adjust separator if needed
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (err) => reject(err));
  });
}

// Function to load data from CSV files
async function loadData() {
  const apoData = await parseCSV('./data/apo-lt-data.csv');
  const matches = await parseCSV('./data/matches.csv');

  console.log('APO Data:', apoData);
  console.log('Matches:', matches);

  return { apoData, matches };
}

// Function to map related manufacturers
function mapRelatedManufacturers(matches, apoData) {
  const manufacturerMap = {};

  matches.forEach(match => {
    const mainProduct = apoData.find(p => p.source_id === match.m_source_id && p.source === match.m_source);
    const compProduct = apoData.find(p => p.source_id === match.c_source_id && p.source === match.c_source);

    console.log('Main Product:', mainProduct);
    console.log('Comp Product:', compProduct);

    if (mainProduct && compProduct) {
      const mainManufacturer = mainProduct.manufacturer;
      const compManufacturer = compProduct.manufacturer;

      console.log('Main Manufacturer:', mainManufacturer);
      console.log('Comp Manufacturer:', compManufacturer);

      if (mainManufacturer && compManufacturer) {
        if (!manufacturerMap[mainManufacturer]) {
          manufacturerMap[mainManufacturer] = new Set();
        }
        manufacturerMap[mainManufacturer].add(compManufacturer);
      }
    }
  });

  return manufacturerMap;
}

// Function to assign relation types
function assignRelationType(manufacturerMap) {
  const relations = [];

  Object.keys(manufacturerMap).forEach(parent => {
    manufacturerMap[parent].forEach(child => {
      relations.push({ parent, child, relation_type: 'sibling' });
    });
  });

  return relations;
}

// Function to save mapped data to CSV
function saveToCSV(manufacturerRelations) {
  if (manufacturerRelations.length === 0) {
    console.error('No manufacturer relations found to save.');
    return;
  }

  const fields = ['parent', 'child', 'relation_type'];  // Specify the fields for CSV
  const csvData = parse(manufacturerRelations, { fields });

  fs.writeFileSync('./output/manufacturerRelations.csv', csvData);
  console.log('CSV file has been saved.');
}

// Main function to execute the process
async function main() {
  const { apoData, matches } = await loadData();

  const manufacturerMap = mapRelatedManufacturers(matches, apoData);

  console.log('Manufacturer Map:', manufacturerMap);

  const manufacturerRelations = assignRelationType(manufacturerMap);

  console.log('Manufacturer Relations:', manufacturerRelations);

  saveToCSV(manufacturerRelations);
}

main();
