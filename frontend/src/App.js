import React, { useState } from 'react';
import {
  Container,
  Paper,
  Typography,
  Box,
  Stepper,
  Step,
  StepLabel,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  ListItemText,
  OutlinedInput,
  CircularProgress,
} from '@mui/material';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import axios from 'axios';

const steps = ['Connection', 'Source Selection', 'Column Selection', 'Ingestion'];

function App() {
  const [activeStep, setActiveStep] = useState(0);
  const [connection, setConnection] = useState({
    Host: 'localhost',
    Port: '9000',
    Database: 'default',
    User: 'default',
    'JWT Token': 'password',
  });
  const [sourceType, setSourceType] = useState('');
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [file, setFile] = useState(null);
  const [result, setResult] = useState(null);

  const handleConnectionChange = (event) => {
    setConnection({
      ...connection,
      [event.target.name]: event.target.value,
    });
  };

  const handleConnect = async () => {
    try {
      setLoading(true);
      const response = await axios.post('http://localhost:8000/api/connect', connection);
      toast.success('Connected successfully!');
      setActiveStep(1);
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleSourceTypeChange = async (event) => {
    const type = event.target.value;
    setSourceType(type);
    if (type === 'clickhouse') {
      try {
        setLoading(true);
        const response = await axios.post('http://localhost:8000/api/tables', connection);
        setTables(response.data.tables);
      } catch (error) {
        toast.error(error.response?.data?.detail || 'Failed to fetch tables');
      } finally {
        setLoading(false);
      }
    }
  };

  const handleTableChange = async (event) => {
    const table = event.target.value;
    setSelectedTable(table);
    try {
      setLoading(true);
      const response = await axios.post(`http://localhost:8000/api/columns?table=${table}`, connection);
      setColumns(response.data.columns);
      setSelectedColumns([]);
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to fetch columns');
    } finally {
      setLoading(false);
    }
  };

  const handleColumnChange = (event) => {
    setSelectedColumns(event.target.value);
  };

  const handleFileChange = (event) => {
    setFile(event.target.files[0]);
  };

  const handleIngest = async () => {
    try {
      setLoading(true);
      let response;

      if (sourceType === 'clickhouse') {
        response = await axios.post('http://localhost:8000/api/ingest/clickhouse-to-file', {
          ...connection,
          columns: selectedColumns,
          table: selectedTable,
        });
      } else {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('columns', JSON.stringify(selectedColumns));
        response = await axios.post('http://localhost:8000/api/ingest/file-to-clickhouse', formData, {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
        });
      }

      setResult(response.data);
      toast.success(`Successfully processed ${response.data.record_count} records`);
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Ingestion failed');
    } finally {
      setLoading(false);
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box sx={{ mt: 2 }}>
            <TextField
              fullWidth
              label="Host"
              name="host"
              value={connection.host}
              onChange={handleConnectionChange}
              margin="normal"
            />
            <TextField
              fullWidth
              label="Port"
              name="port"
              type="number"
              value={connection.port}
              onChange={handleConnectionChange}
              margin="normal"
            />
            <TextField
              fullWidth
              label="Database"
              name="database"
              value={connection.database}
              onChange={handleConnectionChange}
              margin="normal"
            />
            <TextField
              fullWidth
              label="User"
              name="user"
              value={connection.user}
              onChange={handleConnectionChange}
              margin="normal"
            />
            <TextField
              fullWidth
              label="JWT Token"
              name="jwt_token"
              value={connection.jwt_token}
              onChange={handleConnectionChange}
              margin="normal"
            />
            <Button
              variant="contained"
              onClick={handleConnect}
              disabled={loading}
              sx={{ mt: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Connect'}
            </Button>
          </Box>
        );

      case 1:
        return (
          <Box sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Source Type</InputLabel>
              <Select
                value={sourceType}
                onChange={handleSourceTypeChange}
                label="Source Type"
              >
                <MenuItem value="clickhouse">ClickHouse</MenuItem>
                <MenuItem value="file">Flat File</MenuItem>
              </Select>
            </FormControl>
            {sourceType === 'clickhouse' && (
              <FormControl fullWidth sx={{ mt: 2 }}>
                <InputLabel>Table</InputLabel>
                <Select
                  value={selectedTable}
                  onChange={handleTableChange}
                  label="Table"
                >
                  {tables.map((table) => (
                    <MenuItem key={table} value={table}>
                      {table}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}
            {sourceType === 'file' && (
              <Box sx={{ mt: 2 }}>
                <input
                  accept=".csv,.json"
                  style={{ display: 'none' }}
                  id="file-upload"
                  type="file"
                  onChange={handleFileChange}
                />
                <label htmlFor="file-upload">
                  <Button variant="contained" component="span">
                    Upload File
                  </Button>
                </label>
                {file && (
                  <Typography sx={{ mt: 1 }}>
                    Selected file: {file.name}
                  </Typography>
                )}
              </Box>
            )}
          </Box>
        );

      case 2:
        return (
          <Box sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Columns</InputLabel>
              <Select
                multiple
                value={selectedColumns}
                onChange={handleColumnChange}
                input={<OutlinedInput label="Columns" />}
                renderValue={(selected) => selected.join(', ')}
              >
                {columns.map((column) => (
                  <MenuItem key={column.name} value={column.name}>
                    <Checkbox checked={selectedColumns.indexOf(column.name) > -1} />
                    <ListItemText primary={column.name} />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>
        );

      case 3:
        return (
          <Box sx={{ mt: 2 }}>
            <Button
              variant="contained"
              onClick={handleIngest}
              disabled={loading || !selectedColumns.length}
            >
              {loading ? <CircularProgress size={24} /> : 'Start Ingestion'}
            </Button>
            {result && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="h6">Results:</Typography>
                <Typography>Records processed: {result.record_count}</Typography>
                <Typography>
                  {result.filename
                    ? `File saved as: ${result.filename}`
                    : `Data imported to table: ${result.table}`}
                </Typography>
              </Box>
            )}
          </Box>
        );

      default:
        return null;
    }
  };

  return (
    <Container maxWidth="md">
      <Paper sx={{ p: 4, mt: 4 }}>
        <Typography variant="h4" gutterBottom>
          ClickHouse-FlatFile Data Ingestion Tool
        </Typography>
        <Stepper activeStep={activeStep} sx={{ mt: 4 }}>
          {steps.map((label) => (
            <Step key={label}>
              <StepLabel>{label}</StepLabel>
            </Step>
          ))}
        </Stepper>
        {renderStepContent(activeStep)}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 4 }}>
          <Button
            disabled={activeStep === 0}
            onClick={() => setActiveStep((prev) => prev - 1)}
          >
            Back
          </Button>
          <Button
            variant="contained"
            onClick={() => setActiveStep((prev) => prev + 1)}
            disabled={activeStep === steps.length - 1}
          >
            Next
          </Button>
        </Box>
      </Paper>
      <Box sx={{ textAlign: 'center', mt: 4, mb: 2 }}>
        <Typography variant="body2" color="text.secondary">
          Made with ❤️ by Rimple
        </Typography>
      </Box>
      <ToastContainer />
    </Container>
  );
}

export default App; 