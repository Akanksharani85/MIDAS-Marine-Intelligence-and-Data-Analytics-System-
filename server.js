// server.js (with API endpoint)

require('dotenv').config();
const express = require('express');
const path = require('path');
const multer = require('multer');
const admin = require('firebase-admin');

// ==> NEW: Import the pg package
const { Pool } = require('pg');

// =================================================================
// FIREBASE SETUP
const serviceAccount = require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  storageBucket: 'aqua-ai-platform.firebasestorage.app' // <-- No extra spaces
});

const bucket = admin.storage().bucket();
const db = admin.firestore();
// =================================================================

// ==> NEW: Setup the database connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

const app = express();
const PORT = process.env.PORT || 3000;

const upload = multer({ storage: multer.memoryStorage() });

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});
app.use(express.static(path.join(__dirname, 'public')));

// API for file upload (This remains the same)
// ==> UPDATED: API for file upload to include schema metadata
// ==> UPDATED: API for file upload to use PostgreSQL for metadata
// ==> REVERTED: API for simple file upload
app.post('/api/upload', upload.single('dataFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file was uploaded.');
  }

  const blob = bucket.file(`datasets/${Date.now()}-${req.file.originalname}`);
  
  const blobStream = blob.createWriteStream({
    metadata: {
      contentType: req.file.mimetype,
    },
  });

  blobStream.on('error', (err) => res.status(500).json({ message: err.message }));

  blobStream.on('finish', async () => {
    try {
      // Create a record in the PostgreSQL "Datasets" table
      const insertQuery = `
        INSERT INTO "Datasets" (original_filename, storage_path, data_type, file_size_bytes) 
        VALUES ($1, $2, $3, $4)`;
      const values = [req.file.originalname, blob.name, 'Oceanographic', req.file.size];
      await pool.query(insertQuery, values);

      res.status(200).json({ 
        message: 'File uploaded! Processing will begin shortly.',
        filename: blob.name 
      });
    } catch (dbError) {
      console.error('Error creating dataset record in DB:', dbError);
      res.status(500).json({ message: 'Failed to create dataset record.' });
    }
  });

  blobStream.end(req.file.buffer);
});

// NEW: Import csv-parser and stream
const csv = require('csv-parser');
const { Readable } = require('stream');

// ... (your other code)

// ==> NEW: API endpoint to analyze a CSV and detect its schema
app.post('/api/analyze-csv', upload.single('dataFile'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file was uploaded.');
    }

    const results = [];
    const headers = [];
    let headerLock = false;

    // Create a readable stream from the uploaded file buffer
    const stream = Readable.from(req.file.buffer);

    stream
        .pipe(csv())
        .on('headers', (headerList) => {
            // Capture header names
            headers.push(...headerList);
            headerLock = true;
        })
        .on('data', (data) => {
            // Read only the first 5 rows to analyze data types
            if (results.length < 5) {
                results.push(data);
            }
        })
        .on('end', () => {
            if (headers.length === 0) {
                return res.status(400).json({ error: 'Could not parse CSV headers.' });
            }

            // Function to guess the data type of a column
            const guessDataType = (columnName) => {
                let isNumeric = true;
                let isDate = true;

                for (const row of results) {
                    const value = row[columnName];
                    if (value === null || value === '') continue; // Skip empty values

                    // Check if it's a number
                    if (isNaN(Number(value))) {
                        isNumeric = false;
                    }
                    // Check if it can be parsed as a date
                    if (isNaN(Date.parse(value))) {
                        isDate = false;
                    }
                }

                if (isNumeric) return 'Numeric';
                if (isDate) return 'Date/Time';
                return 'Categorical (Text)';
            };

            const schema = headers.map(header => ({
                columnName: header,
                dataType: guessDataType(header)
            }));
            
            res.json(schema);
        });
});

// ==> UPDATED: API endpoint to get oceanographic data with filtering
app.get('/api/oceanographic-data', async (req, res) => {
  // Get the location from the query parameter (e.g., ?location=Goa%20Coast)
  const { location } = req.query;

  let query = 'SELECT * FROM "OceanographicData"';
  const queryParams = [];

  // If a location is provided, add a WHERE clause to the query
  if (location) {
    query += ' WHERE "Location" = $1';
    queryParams.push(location);
  }

  query += ' ORDER BY "Date" DESC'; // Show newest data first

  try {
    const client = await pool.connect();
    // Use parameterized query for security
    const result = await client.query(query, queryParams);
    res.json(result.rows);
    client.release();
  } catch (err) {
    console.error('Error executing query', err.stack);
    res.status(500).send("Error connecting to database");
  }
});

// ==> NEW: API endpoint for the policymaker's summary data
// ==> UPDATED: API endpoint for location-based summaries
app.get('/api/policy-summary', async (req, res) => {
  try {
    const client = await pool.connect();
    const query = `
  SELECT
    "Location",
    COUNT(*) AS record_count,
    ROUND(AVG("Temperature_Celsius"), 2) AS avg_temp,
    ROUND(AVG("Salinity_PSU"), 2) AS avg_salinity,
    MAX("Date") AS last_updated
  FROM "OceanographicData"
  GROUP BY "Location"
  ORDER BY "Location";
`;
    const result = await client.query(query);
    res.json(result.rows);
    client.release();
  } catch (err) {
    console.error('Error executing summary query', err.stack);
    res.status(500).send("Error connecting to database");
  }
});

app.listen(PORT, () => {
  console.log(`Server http://localhost:${PORT} par chal raha hai`);
});