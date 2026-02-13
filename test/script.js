// Global variables
console.log("JavaScript file loaded successfully");
let socket = null;
let isConnected = false;
let isPaused = false;
let logs = [];
let pauseBuffer = []; // Buffer logs while paused
let currentTab = "comparison";
let currentTableMode = "single";
let jobComplete = false;
let reportUrl = "";

// Primary Key dropdown HTML generator (reusable)
function generatePrimaryKeyDropdown(prefix, focusColor) {
  const borderColor = focusColor || "red-500";
  const ringColor = focusColor.replace("500", "200") || "red-200";
  return `
    <div class="form-group">
        <label for="${prefix}PrimaryKey" class="block text-sm font-medium mb-2">🔑 Primary Key</label>
        <select id="${prefix}PrimaryKey" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-${borderColor} focus:ring-2 focus:ring-${ringColor}" onchange="handlePrimaryKeyDropdownChange('${prefix}')">
            <option value="N/A" selected>N/A</option>
            <option value="key columns present but details unknown">key columns present but details unknown</option>
            <option value="manual">Enter Manually</option>
        </select>
        <div id="${prefix}PrimaryKeyManualContainer" class="hidden mt-2">
            <input type="text" id="${prefix}PrimaryKeyManual" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-${borderColor} focus:ring-2 focus:ring-${ringColor}" placeholder="Enter primary key column(s), e.g., ID or ID,COUNTRY_CODE" maxlength="500">
            <p class="text-sm text-gray-500 mt-1">For composite keys, enter comma-separated column names</p>
        </div>
    </div>`;
}

// Handle PK dropdown change - show/hide manual input
function handlePrimaryKeyDropdownChange(prefix) {
  const select = document.getElementById(prefix + "PrimaryKey");
  const manualContainer = document.getElementById(prefix + "PrimaryKeyManualContainer");
  const manualInput = document.getElementById(prefix + "PrimaryKeyManual");
  if (!select || !manualContainer) return;
  if (select.value === "manual") {
    manualContainer.classList.remove("hidden");
  } else {
    manualContainer.classList.add("hidden");
    if (manualInput) manualInput.value = "";
  }
}

// Resolve PK value from dropdown + manual input
function resolvePrimaryKeyValue(prefix) {
  const select = document.getElementById(prefix + "PrimaryKey");
  if (!select) return "";
  if (select.value === "manual") {
    const manual = document.getElementById(prefix + "PrimaryKeyManual");
    return manual ? manual.value.trim() : "";
  }
  return select.value;
}

// SSE Execution State
let currentRunId = null;
let eventSource = null;
let executionStatus = "IDLE"; // IDLE, RUNNING, SUCCESS, FAILED, STOPPED

// Data Load specific UI state (isolated from other tabs)
let dataLoadRunStatus = "IDLE";
let dataLoadButtonLabel = "Execute Data Load";

// Tab name to API tab mapping
const TAB_API_MAP = {
  comparison: "data_comparison",
  load: "data_load",
  schema: "schema_generation",
  fileload: "file_load",
  filedownload: "file_download",
  mismatch: "mismatch_explorer",
};

// Database configurations matching React component
const databaseConfigs = {
  PostgreSQL: {
    hosts: ["localhost", "postgres-server.local", "db.example.com"],
    ports: ["5432", "5433", "5434"],
    usernames: ["postgres", "admin", "dbuser"],
    databases: ["postgres", "myapp_db", "production_db", "staging_db"],
  },
  MySQL: {
    hosts: ["localhost", "mysql-server.local", "db.example.com"],
    ports: ["3306", "3307", "3308"],
    usernames: ["root", "mysql", "admin", "dbuser"],
    databases: ["mysql", "myapp_db", "production_db", "staging_db"],
  },
  Oracle: {
    hosts: ["localhost", "oracle-server.local", "db.example.com"],
    ports: ["1521", "1522", "1523"],
    usernames: ["system", "oracle", "admin", "dbuser"],
    databases: ["XE", "ORCL", "ORCLPDB", "production_db"],
  },
  "SQL Server": {
    hosts: ["localhost", "sqlserver.local", "db.example.com"],
    ports: ["1433", "1434", "1435"],
    usernames: ["sa", "admin", "dbuser"],
    databases: ["master", "myapp_db", "production_db", "staging_db"],
  },
  MongoDB: {
    hosts: ["localhost", "mongo-server.local", "db.example.com"],
    ports: ["27017", "27018", "27019"],
    usernames: ["admin", "root", "mongouser"],
    databases: ["admin", "myapp_db", "production_db", "staging_db"],
  },
  Snowflake: {
    hosts: [
      "account.snowflakecomputing.com",
      "account.us-east-1.snowflakecomputing.com",
    ],
    ports: ["443"],
    usernames: ["admin", "snowflake_user"],
    databases: ["DEMO_DB", "PRODUCTION_DB", "STAGING_DB", "ANALYTICS_DB"],
  },
  Hive: {
    hosts: ["hive-server.local", "hive.example.com", "localhost"],
    ports: ["10000", "10001"],
    usernames: ["hive", "hadoop", "admin"],
    databases: ["default", "warehouse", "staging", "production"],
  },
};

const databaseTypes = [
  "PostgreSQL",
  "MySQL",
  "Oracle",
  "SQL Server",
  "MongoDB",
  "Snowflake",
  "Hive",
  "Teradata",
  "File",
];

// Common IANA time zones for dropdown
const commonTimeZones = [
  "N/A",
  "UTC",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Phoenix",
  "America/Los_Angeles",
  "America/Anchorage",
  "Pacific/Honolulu",
  "Europe/London",
  "Europe/Paris",
  "Europe/Berlin",
  "Europe/Moscow",
  "Asia/Dubai",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Asia/Tokyo",
  "Asia/Shanghai",
  "Asia/Hong_Kong",
  "Australia/Sydney",
  "Australia/Melbourne",
  "Pacific/Auckland",
  "Africa/Johannesburg",
  "America/Sao_Paulo",
  "America/Mexico_City",
  "America/Toronto",
  "Europe/Amsterdam",
  "Europe/Zurich",
  "Asia/Seoul",
  "Asia/Jakarta"
];

// Initialize when page loads
document.addEventListener("DOMContentLoaded", function () {
  console.log("Data Management Platform loaded");
  console.log("Starting initialization...");

  // Initialize tab switching
  console.log("Initializing tabs...");
  initializeTabs();
  
  // Initialize download section visibility for initial tab (comparison is default)
  updateDownloadSectionVisibility(currentTab);

  // Initialize log controls
  console.log("Initializing log controls...");
  initializeLogControls();

  // Initialize table mode selection
  console.log("Initializing table mode selection...");
  initializeTableModeSelection();

  // Generate initial forms
  console.log("Generating comparison form...");
  generateComparisonForm();
  console.log("Generating data load form...");
  generateDataLoadForm();
  console.log("Generating schema generation form...");
  generateSchemaGenerationForm();
  console.log("Generating file load form...");
  generateFileLoadForm();
  console.log("Generating file download form...");
  generateFileDownloadForm();
  console.log("Generating mismatch explorer form...");
  generateMismatchExplorerForm();

  // Initialize WebSocket connection (legacy - kept for backwards compatibility)
  console.log("Initializing WebSocket...");
  initializeWebSocket();

  // Initialize comparison type selection
  console.log("Initializing comparison type selection...");
  initializeComparisonTypeSelection();

  // Update connection status to show ready
  updateConnectionStatus();

  console.log("Initialization complete");
});

// Initialize table mode selection functionality
function initializeTableModeSelection() {
  const radioButtons = document.querySelectorAll('input[name="tableMode"]');
  radioButtons.forEach((radio) => {
    radio.addEventListener("change", function () {
      currentTableMode = this.value;
      console.log("Table mode changed to:", currentTableMode);
      handleTableModeChange(this.value);
    });
  });
}

// Initialize comparison type selection functionality
function initializeComparisonTypeSelection() {
  const comparisonTypeSelect = document.getElementById(
    "comparison-type-select"
  );
  if (comparisonTypeSelect) {
    comparisonTypeSelect.addEventListener("change", function () {
      const selectedType = this.value;
      console.log("Comparison type changed to:", selectedType);
      handleComparisonTypeChange(selectedType);
    });
  }
}

// Handle comparison type changes
function handleComparisonTypeChange(comparisonType) {
  const tableModeSection = document.getElementById("table-mode-selection");
  const comparisonForm = document.getElementById("comparison-form");

  // Show table mode selection only for database-to-database
  if (comparisonType === "database-to-database") {
    tableModeSection.style.display = "block";
  } else {
    tableModeSection.style.display = "none";
  }

  // Regenerate the comparison form based on the selected type
  if (comparisonType) {
    generateComparisonFormByType(comparisonType);
  } else {
    comparisonForm.innerHTML =
      '<p class="text-gray-500 text-center py-4">Please select a comparison type to continue</p>';
  }
}

// Tab switching functionality
function initializeTabs() {
  const tabButtons = document.querySelectorAll(".tab-button");
  const tabContents = document.querySelectorAll(".tab-content");

  console.log("Found tab buttons:", tabButtons.length);
  console.log("Found tab contents:", tabContents.length);

  tabButtons.forEach((button) => {
    console.log("Attaching event to button:", button.dataset.tab);
    button.addEventListener("click", function () {
      const targetTab = this.dataset.tab;
      console.log("Tab clicked:", targetTab);

      // Remove active class from all buttons and contents
      tabButtons.forEach((btn) => btn.classList.remove("active"));
      tabContents.forEach((content) => content.classList.remove("active"));

      // Add active class to clicked button and corresponding content
      this.classList.add("active");
      const targetContent = document.getElementById(targetTab);
      if (targetContent) {
        targetContent.classList.add("active");
        currentTab = targetTab;
        console.log("Tab switched to:", targetTab);
        
        // Update Real-time Logs title based on active tab
        updateLogTitle(targetTab);
        
        // Show/hide download button section based on tab
        updateDownloadSectionVisibility(targetTab);
      } else {
        console.error("Target content not found for tab:", targetTab);
      }
    });
  });
}

// Update the Real-time Logs title based on the active tab
function updateLogTitle(tabId) {
  const logTitleElement = document.getElementById("log-title");
  if (!logTitleElement) return;
  
  // Map tab IDs to display names
  const tabTitles = {
    "comparison": "Data Comparison",
    "load": "Data Load",
    "schema": "Schema Generation",
    "fileload": "File Load",
    "filedownload": "File Download",
    "mismatch": "Mismatch Explorer"
  };
  
  const tabDisplayName = tabTitles[tabId] || "Data Comparison";
  logTitleElement.textContent = `Real-time Logs - ${tabDisplayName}`;
  console.log("[UI] Log title updated to Real-time Logs - " + tabDisplayName);
}

// Show/hide download sections based on active tab
// Data Comparison download section, File Download download section, Schema Generation download section
function updateDownloadSectionVisibility(tabId) {
  const downloadSection = document.getElementById("download-report-section");
  const fileDownloadSection = document.getElementById("filedownload-download-section");
  const schemaDownloadSection = document.getElementById("schema-download-section");
  
  // Handle Data Comparison download section
  if (downloadSection) {
    if (tabId === "comparison") {
      downloadSection.classList.remove("hidden");
      console.log("[UI] Download section shown (comparison tab)");
    } else {
      downloadSection.classList.add("hidden");
      console.log("[UI] Download section hidden (tab=" + tabId + ")");
    }
  }
  
  // Handle File Download download section (only visible on filedownload tab when download is ready)
  if (fileDownloadSection) {
    if (tabId !== "filedownload") {
      // Hide when not on File Download tab
      fileDownloadSection.classList.add("hidden");
    }
    // Note: The section visibility is managed by startFileDownloadLogStreaming on success
  }
  
  // Handle Schema Generation download section (only visible on schema tab when download is ready)
  if (schemaDownloadSection) {
    if (tabId !== "schema") {
      // Hide when not on Schema Generation tab
      schemaDownloadSection.classList.add("hidden");
    }
    // Note: The section visibility is managed by startSchemaGenerationLogStreaming on success
  }
}

// Generate enhanced dropdown with manual entry option
function generateDropdownWithCustom(
  id,
  options,
  placeholder,
  currentValue = ""
) {
  const hasManualEntry = id.includes("Manual");

  if (hasManualEntry) {
    return `
            <div class="input-with-dropdown">
                <input type="text" 
                       id="${id}" 
                       class="form-input" 
                       placeholder="${placeholder}" 
                       value="${currentValue}"
                       maxlength="1500">
                <button type="button" 
                        class="back-button" 
                        onclick="switchToDropdown('${id}')">
                    ← Back to dropdown
                </button>
            </div>
        `;
  }

  return `
        <select id="${id}" class="form-select">
            <option value="">${placeholder}</option>
            ${options
              .map(
                (option) =>
                  `<option value="${option}" ${
                    option === currentValue ? "selected" : ""
                  }>${option}</option>`
              )
              .join("")}
            <option value="custom">Enter manually...</option>
        </select>
    `;
}

// Handle dropdown to manual switch
function handleDropdownChange(selectId, fieldType) {
  const select = document.getElementById(selectId);
  if (select.value === "custom") {
    const manualId = selectId + "Manual";
    const placeholder = `Enter custom ${fieldType}`;

    select.parentElement.innerHTML = generateDropdownWithCustom(
      manualId,
      [],
      placeholder
    );

    // Focus the new input
    const newInput = document.getElementById(manualId);
    if (newInput) {
      newInput.focus();
    }
  }
}

// Switch back to dropdown from manual entry
function switchToDropdown(manualId) {
  const input = document.getElementById(manualId);
  const baseId = manualId.replace("Manual", "");
  const fieldType = getFieldType(baseId);
  const dbType = getSelectedDatabaseType(baseId);

  let options = [];
  if (dbType && databaseConfigs[dbType]) {
    if (fieldType === "host") options = databaseConfigs[dbType].hosts;
    else if (fieldType === "port") options = databaseConfigs[dbType].ports;
    else if (fieldType === "username")
      options = databaseConfigs[dbType].usernames;
    else if (fieldType === "database")
      options = databaseConfigs[dbType].databases;
  }

  const placeholder = `Select ${fieldType}`;
  input.parentElement.innerHTML = generateDropdownWithCustom(
    baseId,
    options,
    placeholder
  );

  // Re-attach event listener
  const newSelect = document.getElementById(baseId);
  if (newSelect) {
    newSelect.addEventListener("change", function () {
      handleDropdownChange(this.id, fieldType);
    });
  }
}

// Get field type from ID
function getFieldType(id) {
  if (id.includes("Host")) return "host";
  if (id.includes("Port")) return "port";
  if (id.includes("Username")) return "username";
  if (id.includes("Database")) return "database";
  return "";
}

// Get selected database type for a field
function getSelectedDatabaseType(fieldId) {
  const prefix = fieldId.split(/Host|Port|Username|Database/)[0];
  const typeSelectId = prefix + "Type";
  const typeSelect = document.getElementById(typeSelectId);
  return typeSelect ? typeSelect.value : "";
}

// Generate form fields for source or target
function generateFormFields(prefix, isFileTab = false) {
  const isSource = prefix.includes("source") || prefix.includes("Source");
  const cardType = isSource ? "source" : "target";
  const cardTitle = isSource ? "Source Configuration" : "Target Configuration";

  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    ${cardTitle}
                </h3>
                <p>Configure your data ${
                  isSource ? "source" : "target"
                } connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${databaseTypes
                          .map(
                            (type) => `<option value="${type}">${type}</option>`
                          )
                          .join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <div id="${prefix}HostContainer">
                            <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <div id="${prefix}PortContainer">
                            <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                        </div>
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <div id="${prefix}DatabaseContainer">
                        <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                    </div>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <div id="${prefix}UsernameContainer">
                            <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                ${
                  !isFileTab
                    ? `
                <div class="form-group">
                    <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
                    <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
                </div>
                
                ${generatePrimaryKeyDropdown(prefix, "red-500")}
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
                `
                    : ""
                }
                
                ${
                  isFileTab
                    ? `
                <div class="form-group">
                    <label for="${prefix}ExcelFile" class="block text-sm font-medium mb-2">📁 Excel File</label>
                    <input type="file" id="${prefix}ExcelFile" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg hover:border-red-500 cursor-pointer" accept=".xlsx,.xls,.csv">
                </div>
                `
                    : ""
                }
            </div>
        </div>
    `;
}

// Generate form fields with file upload (for multi-table mode)
function generateFormFieldsWithFileUpload(prefix) {
  const isSource = prefix.includes("source") || prefix.includes("Source");
  const cardType = isSource ? "source" : "target";
  const cardTitle = isSource ? "Source Configuration" : "Target Configuration";
  const fileFieldId = isSource ? "sourceExcelFile" : "targetExcelFile";
  const fileLabel = isSource ? "Source Excel File" : "Target Excel File";
  const fileDescription = isSource 
    ? "Upload Excel with source_table_name, source_sql, source_key_columns columns"
    : "Upload Excel with target_table_name, target_sql, target_key_columns columns";

  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    ${cardTitle}
                </h3>
                <p>Configure your data ${
                  isSource ? "source" : "target"
                } connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${databaseTypes
                          .map(
                            (type) => `<option value="${type}">${type}</option>`
                          )
                          .join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <div id="${prefix}HostContainer">
                            <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <div id="${prefix}PortContainer">
                            <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                        </div>
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <div id="${prefix}DatabaseContainer">
                        <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                    </div>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <div id="${prefix}UsernameContainer">
                            <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${fileFieldId}" class="block text-sm font-medium mb-2">📁 ${fileLabel}</label>
                    <input type="file" id="${fileFieldId}" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg hover:border-red-500 cursor-pointer" accept=".xlsx,.xls">
                    <p class="text-sm text-gray-600 mt-1">${fileDescription}</p>
                </div>
            </div>
        </div>
    `;
}

// Generate Unix server section
function generateUnixServerSection(prefix) {
   return `
         <div class="unix-server-section mb-6">
             <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
                 <div class="card-header gradient-header section-header-unix">
                     <h3 class="text-wellsfargo-red flex items-center gap-2">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z"></path>
                        </svg>
                        Unix Server Configuration
                    </h3>
                    <p>Server details for operation execution</p>
                </div>
                <div class="config-content p-6 space-y-4">
                    <div class="grid grid-cols-2 gap-4">
                        <div class="form-group">
                            <label for="${prefix}UnixHost" class="block text-sm font-medium mb-2">🖥️ Unix Host</label>
                            <input type="text" id="${prefix}UnixHost" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring-2 focus:ring-blue-200" placeholder="Enter Unix server host" maxlength="255">
                        </div>
                        
                        <div class="form-group">
                            <label for="${prefix}UserEmail" class="block text-sm font-medium mb-2">📧 User Email <span class="text-red-500">*</span></label>
                            <input type="email" id="${prefix}UserEmail" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring-2 focus:ring-blue-200" placeholder="Enter email address" required>
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}BatchId" class="block text-sm font-medium mb-2">🆔 Batch ID</label>
                        <input type="text" id="${prefix}BatchId" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring-2 focus:ring-blue-200" placeholder="Enter Batch ID" maxlength="100">
                    </div>
                    
                    <div class="grid grid-cols-2 gap-4">
                        <div class="form-group">
                            <label for="${prefix}UnixUsername" class="block text-sm font-medium mb-2">👤 Unix Username</label>
                            <input type="text" id="${prefix}UnixUsername" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring-2 focus:ring-blue-200" placeholder="Enter Unix username" maxlength="100">
                        </div>
                        
                        <div class="form-group">
                            <label for="${prefix}UnixPassword" class="block text-sm font-medium mb-2">🔒 Unix Password</label>
                            <input type="password" id="${prefix}UnixPassword" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring-2 focus:ring-blue-200" placeholder="Enter Unix password" maxlength="100">
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Generate Time Zone field section for comparison forms
function generateTimeZoneSection(prefix) {
   const optionsHtml = commonTimeZones.map(tz => 
     `<option value="${tz}"${tz === 'N/A' ? ' selected' : ''}>${tz}</option>`
   ).join('');
   
   return `
         <div class="time-zone-section mb-6">
             <div class="source-target-card border-t-4 border-t-wellsfargo-gold shadow-elevated">
                 <div class="card-header gradient-header section-header-timezone">
                     <h3 class="text-wellsfargo-gold flex items-center gap-2">
                        <span class="text-xl">🕐</span>
                        Time Zone Configuration
                    </h3>
                    <p>Specify time zone for data comparison</p>
                </div>
                <div class="config-content p-6">
                    <div class="form-group">
                        <label for="${prefix}TimeZone" class="block text-sm font-medium mb-2">🌍 Time Zone</label>
                        <div class="relative">
                            <select id="${prefix}TimeZone" 
                                   class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200"
                                   onchange="handleTimeZoneChange('${prefix}')">
                                ${optionsHtml}
                                <option value="__manual__">Enter Manually</option>
                            </select>
                        </div>
                        <div id="${prefix}TimeZoneManualContainer" class="mt-2 hidden">
                            <input type="text" 
                                   id="${prefix}TimeZoneManual" 
                                   class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" 
                                   placeholder="Enter custom time zone (e.g., America/Phoenix)"
                                   maxlength="100">
                        </div>
                        <p class="text-sm text-gray-500 mt-1">Select from the list or choose "Enter Manually" to type a custom time zone. Leave as "N/A" if not applicable.</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Handle Time Zone dropdown change (show/hide manual input)
function handleTimeZoneChange(prefix) {
  const select = document.getElementById(prefix + 'TimeZone');
  const manualContainer = document.getElementById(prefix + 'TimeZoneManualContainer');
  const manualInput = document.getElementById(prefix + 'TimeZoneManual');
  if (!select || !manualContainer) return;
  
  if (select.value === '__manual__') {
    manualContainer.classList.remove('hidden');
    if (manualInput) manualInput.focus();
  } else {
    manualContainer.classList.add('hidden');
    if (manualInput) manualInput.value = '';
  }
}

// Get effective time zone value (handles manual entry)
function getTimeZoneValue(prefix) {
  const select = document.getElementById(prefix + 'TimeZone');
  if (!select) return 'N/A';
  if (select.value === '__manual__') {
    const manualInput = document.getElementById(prefix + 'TimeZoneManual');
    return (manualInput && manualInput.value.trim()) || 'N/A';
  }
  return select.value || 'N/A';
}

// Update dropdown options based on database type
function updateDropdownOptions(prefix, dbType) {
  const config = databaseConfigs[dbType];
  if (!config) return;

  // Host, Port, Database Name, Username are now plain text inputs (no dropdowns)
  // Just update placeholder text based on database type for guidance
  const hostInput = document.getElementById(prefix + "Host");
  if (hostInput && config.hosts && config.hosts.length > 0) {
    hostInput.placeholder = `e.g., ${config.hosts[0]}`;
  }
  
  const portInput = document.getElementById(prefix + "Port");
  if (portInput && config.ports && config.ports.length > 0) {
    portInput.placeholder = config.ports[0];
  }
  
  const usernameInput = document.getElementById(prefix + "Username");
  if (usernameInput && config.usernames && config.usernames.length > 0) {
    usernameInput.placeholder = `e.g., ${config.usernames[0]}`;
  }
  
  const databaseInput = document.getElementById(prefix + "Database");
  if (databaseInput && config.databases && config.databases.length > 0) {
    databaseInput.placeholder = `e.g., ${config.databases[0]}`;
  }
}

// Generate Data Comparison form
function generateComparisonForm() {
  console.log("Looking for comparison-form container...");
  const container = document.getElementById("comparison-form");
  if (!container) {
    console.error("comparison-form container not found!");
    return;
  }

  console.log("Found comparison-form container, showing initial message...");
  container.innerHTML =
    '<p class="text-gray-500 text-center py-8 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">👆 Please select a comparison type above to continue</p>';
}

// Handle table mode change for Data Comparison
function handleTableModeChange(mode) {
  currentTableMode = mode;
  console.log("Table mode changed to:", mode);

  const container = document.getElementById("comparison-form-fields");
  if (container) {
    // Clear and regenerate form with current table mode
    if (mode === "single") {
      container.innerHTML = `
                <div class="grid lg:grid-cols-2 gap-6">
                    ${generateFormFields("comparisonSource")}
                    ${generateFormFields("comparisonTarget")}
                </div>
                
                ${generateTimeZoneSection("comparison")}
                
                ${generateUnixServerSection("comparison")}
            `;
    } else {
      // Multi table mode - show database fields AND file upload option
      container.innerHTML = `
                <div class="grid lg:grid-cols-2 gap-6">
                    ${generateFormFieldsWithFileUpload("comparisonSource")}
                    ${generateFormFieldsWithFileUpload("comparisonTarget")}
                </div>
                
                ${generateTimeZoneSection("comparison")}
                
                ${generateUnixServerSection("comparison")}
            `;
    }
    attachFormListeners("comparison");
  }
}

// Generate comparison form based on comparison type
function generateComparisonFormByType(comparisonType) {
  const container = document.getElementById("comparison-form");
  if (!container) return;

  let sourceFields, targetFields;

  // Determine what fields to show for source and target based on comparison type
  switch (comparisonType) {
    case "file-to-file":
      sourceFields = generateFileFields("comparisonSource");
      targetFields = generateFileFields("comparisonTarget");
      break;
    case "file-to-database":
      sourceFields = generateFileFields("comparisonSource");
      targetFields = generateFormFields("comparisonTarget");
      break;
    case "database-to-file":
      sourceFields = generateFormFields("comparisonSource");
      targetFields = generateFileFields("comparisonTarget");
      break;
    case "database-to-database":
      // Use table mode to determine fields
      if (currentTableMode === "single") {
        sourceFields = generateFormFields("comparisonSource");
        targetFields = generateFormFields("comparisonTarget");
      } else {
        sourceFields = generateFormFieldsWithFileUpload("comparisonSource");
        targetFields = generateFormFieldsWithFileUpload("comparisonTarget");
      }
      break;
    default:
      container.innerHTML =
        '<p class="text-gray-500 text-center py-4">Please select a comparison type</p>';
      return;
  }

  container.innerHTML = `
        <form class="space-y-6" onsubmit="executeOperation(event, 'Data Comparison')">
            <div id="comparison-form-fields">
                <div class="grid lg:grid-cols-2 gap-6">
                    ${sourceFields}
                    ${targetFields}
                </div>
                
                ${generateTimeZoneSection("comparison")}
                
                ${generateUnixServerSection("comparison")}
            </div>
            
            <div class="execute-section text-center p-6 border-t border-gray-200">
                <button type="submit" class="execute-button bg-gradient-to-r from-red-600 to-red-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-red-700 hover:to-red-800 transition-all duration-200 shadow-lg">
                    <span class="execute-icon mr-2">▶️</span>
                    Execute Data Comparison
                </button>
            </div>
        </form>
    `;

  attachFormListeners("comparison");

  // Attach file field listeners for conditional display
  if (
    comparisonType === "file-to-file" ||
    comparisonType === "file-to-database"
  ) {
    attachFileFieldListeners("comparisonSource");
  }
  if (
    comparisonType === "file-to-file" ||
    comparisonType === "database-to-file"
  ) {
    attachFileFieldListeners("comparisonTarget");
  }

  // Show key columns field for specific comparison types
  showKeyColumnsForComparison(comparisonType);
}

// Generate file configuration fields for source/target
function generateFileFields(prefix) {
  const label = prefix.includes("Source") ? "Source" : "Target";
  const borderColor = prefix.includes("Source")
    ? "border-wellsfargo-red/20"
    : "border-wellsfargo-gold/20";
  const gradientColor = prefix.includes("Source")
    ? "from-wellsfargo-red/10 to-wellsfargo-gold/10"
    : "from-wellsfargo-gold/10 to-wellsfargo-red/10";
  const dotColor = prefix.includes("Source")
    ? "bg-wellsfargo-red"
    : "bg-wellsfargo-gold";
  const titleColor = prefix.includes("Source")
    ? "text-wellsfargo-red"
    : "text-wellsfargo-gold";
  const headerClass = prefix.includes("Source")
    ? "section-header-source"
    : "section-header-target";

  return `
        <div class="card ${borderColor}">
            <div class="card-header gradient-header ${headerClass} bg-gradient-to-r ${gradientColor}">
                <div class="flex items-center gap-2">
                    <div class="w-3 h-3 ${dotColor} rounded-full"></div>
                    <h4 class="${titleColor}">${label} File Configuration</h4>
                </div>
                <p>Configure your ${label.toLowerCase()} file details</p>
            </div>
            <div class="card-content space-y-4">
                <div class="form-group">
                    <label for="${prefix}FileType" class="block text-sm font-medium mb-2">📄 File Type</label>
                    <select id="${prefix}FileType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select file type</option>
                        <option value="csv">CSV</option>
                        <option value="json">JSON</option>
                        <option value="xml">XML</option>
                        <option value="parquet">Parquet</option>
                        <option value="excel">Excel</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}LocationType" class="block text-sm font-medium mb-2">📍 Location Type</label>
                    <select id="${prefix}LocationType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select location type</option>
                        <option value="unix">File Path (Unix)</option>
                        <option value="hadoop">File Path (Hadoop)</option>
                        <option value="upload">Upload File</option>
                    </select>
                </div>
                
                <div id="${prefix}UnixPathField" class="form-group hidden">
                    <label for="${prefix}UnixPath" class="block text-sm font-medium mb-2">📂 Unix File Path</label>
                    <input type="text" id="${prefix}UnixPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="/path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}HadoopPathField" class="form-group hidden">
                    <label for="${prefix}HadoopPath" class="block text-sm font-medium mb-2">📂 Hadoop File Path</label>
                    <input type="text" id="${prefix}HadoopPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="hdfs:///path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}FileUploadField" class="form-group hidden">
                    <label for="${prefix}FileUpload" class="block text-sm font-medium mb-2">📁 Upload File</label>
                    <input type="file" id="${prefix}FileUpload" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg hover:border-red-500 cursor-pointer" accept=".csv,.json,.xml,.parquet,.xlsx,.xls">
                    <p id="${prefix}FileUploadName" class="text-sm text-gray-600 mt-1 hidden"></p>
                </div>
                
                <div id="${prefix}DelimiterField" class="form-group hidden">
                    <label for="${prefix}Delimiter" class="block text-sm font-medium mb-2">✂️ Delimiter</label>
                    <select id="${prefix}Delimiter" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value=",">Comma (,)</option>
                        <option value="|">Pipe (|)</option>
                        <option value="	">Tab</option>
                        <option value=";">Semicolon (;)</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}FileSqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}FileSqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" rows="3" placeholder="Enter SQL query to filter file data (optional, default: N/A)"></textarea>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}KeyColumns" class="block text-sm font-medium mb-2">🔑 Key Columns (for matching)</label>
                    <input type="text" id="${prefix}KeyColumns" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="id,customer_id (comma-separated)" maxlength="500">
                </div>
            </div>
        </div>
    `;
}

// Attach file field listeners for conditional display
function attachFileFieldListeners(prefix) {
  const fileTypeSelect = document.getElementById(prefix + "FileType");
  const locationTypeSelect = document.getElementById(prefix + "LocationType");
  const fileUpload = document.getElementById(prefix + "FileUpload");

  if (fileTypeSelect) {
    fileTypeSelect.addEventListener("change", function () {
      const delimiterField = document.getElementById(prefix + "DelimiterField");
      if (this.value === "csv") {
        delimiterField.classList.remove("hidden");
      } else {
        delimiterField.classList.add("hidden");
      }
    });
  }

  if (locationTypeSelect) {
    locationTypeSelect.addEventListener("change", function () {
      const unixPathField = document.getElementById(prefix + "UnixPathField");
      const hadoopPathField = document.getElementById(prefix + "HadoopPathField");
      const uploadField = document.getElementById(prefix + "FileUploadField");

      // Hide all path fields first
      unixPathField.classList.add("hidden");
      hadoopPathField.classList.add("hidden");
      uploadField.classList.add("hidden");

      if (this.value === "unix") {
        unixPathField.classList.remove("hidden");
      } else if (this.value === "hadoop") {
        hadoopPathField.classList.remove("hidden");
      } else if (this.value === "upload") {
        uploadField.classList.remove("hidden");
      }
    });
  }

  if (fileUpload) {
    fileUpload.addEventListener("change", function () {
      const fileName = document.getElementById(prefix + "FileUploadName");
      if (this.files && this.files[0]) {
        fileName.textContent = `Selected: ${this.files[0].name}`;
        fileName.classList.remove("hidden");
      } else {
        fileName.classList.add("hidden");
      }
    });
  }
}

// Show key columns field for specific comparison types
function showKeyColumnsForComparison(comparisonType) {
  // Key columns are shown for all comparison types that involve files
  // This is already handled in generateFileFields
}

// Generate Data Load form - DB-to-DB only
function generateDataLoadForm() {
  const container = document.getElementById("load-form");
  if (!container) return;

  container.innerHTML = `
        <form class="space-y-6" onsubmit="executeOperation(event, 'Data Load')">
            <!-- Header: Database to Database Load -->
            <div class="mb-6 p-4 border-2 border-dashed border-load/30 rounded-xl bg-gradient-to-br from-card to-load/5 shadow-sm">
                <div class="flex items-center gap-2">
                    <span class="text-2xl">🗄️</span>
                    <span class="text-lg font-semibold text-foreground">Database to Database Load</span>
                </div>
                <p class="text-sm text-gray-500 mt-1">Load data from source database to target database</p>
            </div>
            
            <div id="load-form-fields">
                <div class="grid lg:grid-cols-2 gap-6">
                    ${generateDataLoadSourceFields("loadSource")}
                    ${generateDataLoadTargetFields("loadTarget")}
                </div>
            </div>
            
            ${generateTimeZoneSection("load")}
            
            ${generateUnixServerSection("load")}
            
            <div class="execute-section text-center p-6 border-t border-gray-200">
                <!-- Data Load Status Display -->
                <div id="dataload-status-container" class="mb-4 hidden">
                    <span id="dataload-status-text" class="text-lg font-semibold"></span>
                </div>
                <button type="submit" id="dataload-run-btn" class="execute-button bg-gradient-to-r from-red-600 to-red-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-red-700 hover:to-red-800 transition-all duration-200 shadow-lg">
                    <span class="execute-icon mr-2">▶️</span>
                    <span id="dataload-btn-text">Execute Data Load</span>
                </button>
            </div>
        </form>
    `;

  // Attach event listeners
  attachFormListeners("load");
  attachDataLoadTargetListeners();
}

// Generate Data Load SOURCE fields (database only, no load type)
function generateDataLoadSourceFields(prefix) {
  // Filter out "File" from database types for Data Load
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    Source Configuration
                </h3>
                <p>Configure your source database connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
                    <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
                </div>
                
                ${generatePrimaryKeyDropdown(prefix, "red-500")}
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
            </div>
        </div>
    `;
}

// Generate Data Load TARGET fields (with Load Type + Hive-specific fields)
function generateDataLoadTargetFields(prefix) {
  // Filter out "File" from database types for Data Load
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    Target Configuration
                </h3>
                <p>Configure your target database connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
                    <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
                </div>
                
                ${generatePrimaryKeyDropdown(prefix, "orange-500")}
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
                
                <!-- Load Type (Target only) -->
                <div class="form-group">
                    <label for="${prefix}LoadType" class="block text-sm font-medium mb-2">⚡ Load Type</label>
                    <select id="${prefix}LoadType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200">
                        <option value="">Select load type</option>
                        <option value="overwrite">Overwrite</option>
                        <option value="append">Append</option>
                    </select>
                </div>
                
                <!-- Hive-specific fields (hidden by default) -->
                <div id="${prefix}HiveFields" class="hidden space-y-4 p-4 border-2 border-dashed border-yellow-400 rounded-lg bg-yellow-50">
                    <div class="flex items-center gap-2 mb-2">
                        <span class="text-xl">🐝</span>
                        <span class="text-sm font-semibold text-yellow-700">Hive-Specific Configuration</span>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}HadoopPath" class="block text-sm font-medium mb-2">📂 Hadoop Path <span class="text-red-500">*</span></label>
                        <input type="text" id="${prefix}HadoopPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="/user/hive/warehouse/..." maxlength="1000">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}PartitionId" class="block text-sm font-medium mb-2">🔖 Partition ID</label>
                        <input type="text" id="${prefix}PartitionId" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="e.g., year=2024/month=01 (leave empty for N/A)" maxlength="500">
                        <p class="text-sm text-gray-500 mt-1">Leave empty to default to "N/A"</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Attach Data Load target-specific listeners (for Hive fields toggle)
function attachDataLoadTargetListeners() {
  const targetTypeSelect = document.getElementById("loadTargetType");
  
  if (targetTypeSelect) {
    targetTypeSelect.addEventListener("change", function() {
      const hiveFields = document.getElementById("loadTargetHiveFields");
      
      if (this.value === "Hive") {
        if (hiveFields) hiveFields.classList.remove("hidden");
      } else {
        if (hiveFields) hiveFields.classList.add("hidden");
      }
    });
  }
}

// Generate file fields for Data Load
function generateFileFieldsForLoad(prefix) {
  return `
        <div class="source-target-card border-t-4 border-t-load shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-load flex items-center gap-2">
                    <div class="w-3 h-3 bg-load rounded-full"></div>
                    Source File Configuration
                </h3>
                <p>Configure your source file details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}FileType" class="block text-sm font-medium mb-2">📄 File Type</label>
                    <select id="${prefix}FileType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg">
                        <option value="">Select file type</option>
                        <option value="csv">CSV</option>
                        <option value="json">JSON</option>
                        <option value="xml">XML</option>
                        <option value="parquet">Parquet</option>
                        <option value="excel">Excel</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}LocationType" class="block text-sm font-medium mb-2">📍 Location Type</label>
                    <select id="${prefix}LocationType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg">
                        <option value="">Select location type</option>
                        <option value="path">File Path (Server)</option>
                        <option value="upload">Upload File</option>
                    </select>
                </div>
                
                <div id="${prefix}FilePathField" class="form-group hidden">
                    <label for="${prefix}FilePath" class="block text-sm font-medium mb-2">📂 File Path</label>
                    <input type="text" id="${prefix}FilePath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg" placeholder="/path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}FileUploadField" class="form-group hidden">
                    <label for="${prefix}FileUpload" class="block text-sm font-medium mb-2">📁 Upload File</label>
                    <input type="file" id="${prefix}FileUpload" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg" accept=".csv,.json,.xml,.parquet,.xlsx,.xls">
                    <p id="${prefix}FileUploadName" class="text-sm text-gray-600 mt-1 hidden"></p>
                </div>
            </div>
        </div>
    `;
}

// Attach load file field listeners
function attachLoadFileFieldListeners() {
  const locationTypeSelect = document.getElementById("loadSourceLocationType");
  const fileUpload = document.getElementById("loadSourceFileUpload");

  if (locationTypeSelect) {
    locationTypeSelect.addEventListener("change", function () {
      const pathField = document.getElementById("loadSourceFilePathField");
      const uploadField = document.getElementById("loadSourceFileUploadField");

      if (this.value === "path") {
        pathField.classList.remove("hidden");
        uploadField.classList.add("hidden");
      } else if (this.value === "upload") {
        pathField.classList.add("hidden");
        uploadField.classList.remove("hidden");
      } else {
        pathField.classList.add("hidden");
        uploadField.classList.add("hidden");
      }
    });
  }

  if (fileUpload) {
    fileUpload.addEventListener("change", function () {
      const fileName = document.getElementById("loadSourceFileUploadName");
      if (this.files && this.files[0]) {
        fileName.textContent = `Selected: ${this.files[0].name}`;
        fileName.classList.remove("hidden");
      } else {
        fileName.classList.add("hidden");
      }
    });
  }
}

// Generate Schema Generation form
function generateSchemaGenerationForm() {
  const container = document.getElementById("schema-form");
  if (!container) return;

  container.innerHTML = `
        <form class="space-y-6" onsubmit="executeSchemaGenerationOperation(event)">
            <div class="grid lg:grid-cols-2 gap-6">
                ${generateSchemaGenerationSourceFields("schemaSource")}
                ${generateSchemaGenerationTargetFields("schemaTarget")}
            </div>
            
            ${generateUnixServerSection("schema")}
            
            <div class="execute-section text-center p-6 border-t border-gray-200">
                <button type="submit" id="schema-run-btn" class="execute-button bg-gradient-to-r from-red-600 to-red-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-red-700 hover:to-red-800 transition-all duration-200 shadow-lg">
                    <span class="execute-icon mr-2">▶️</span>
                    <span id="schema-btn-text">Execute Schema Generation</span>
                </button>
            </div>
        </form>
    `;

  // Attach event listeners for schema mode toggle
  attachSchemaGenerationListeners();
}

// Generate Schema Generation SOURCE fields (database with Schema Mode)
function generateSchemaGenerationSourceFields(prefix) {
  // Filter out "File" from database types
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
   return `
         <div class="source-target-card border-t-4 border-t-schema shadow-elevated">
             <div class="card-header gradient-header section-header-source">
                 <h3 class="text-schema flex items-center gap-2">
                     <div class="w-3 h-3 bg-schema rounded-full"></div>
                     Source Configuration
                 </h3>
                 <p>Configure your source database connection details</p>
             </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <!-- Schema Mode dropdown -->
                <div class="form-group">
                    <label for="${prefix}SchemaMode" class="block text-sm font-medium mb-2">📊 Schema Mode <span class="text-red-500">*</span></label>
                    <select id="${prefix}SchemaMode" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200">
                        <option value="">Select schema mode</option>
                        <option value="single">Single</option>
                        <option value="multiple">Multiple</option>
                    </select>
                </div>
                
                <!-- Single Mode: Table Name field -->
                <div id="${prefix}SingleModeFields" class="hidden">
                    <div class="form-group">
                        <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name <span class="text-red-500">*</span></label>
                        <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-purple-500 focus:ring-2 focus:ring-purple-200" placeholder="Enter table name" maxlength="255">
                    </div>
                </div>
                
                <!-- Multiple Mode: File Upload field -->
                <div id="${prefix}MultipleModeFields" class="hidden">
                    <div class="form-group">
                        <label for="${prefix}TableListFile" class="block text-sm font-medium mb-2">📁 Upload Table List File <span class="text-red-500">*</span></label>
                        <input type="file" id="${prefix}TableListFile" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg hover:border-purple-500 cursor-pointer" accept=".txt,.csv,.xlsx,.xls">
                        <p id="${prefix}TableListFileName" class="text-sm text-gray-600 mt-1 hidden"></p>
                        <p class="text-sm text-gray-500 mt-1">Upload a file containing list of table names (txt, csv, or excel)</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Generate Schema Generation TARGET fields (database with Output File Name)
function generateSchemaGenerationTargetFields(prefix) {
  // Filter out "File" from database types
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    Target Configuration
                </h3>
                <p>Configure your target database connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <!-- Output File Name (mandatory) -->
                <div class="form-group">
                    <label for="${prefix}OutputFileName" class="block text-sm font-medium mb-2">📄 Output File Name <span class="text-red-500">*</span></label>
                    <input type="text" id="${prefix}OutputFileName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="schema_output.sql" maxlength="255">
                    <p class="text-sm text-gray-500 mt-1">Name of the generated schema file for download</p>
                </div>
            </div>
        </div>
    `;
}

// Attach Schema Generation specific listeners
function attachSchemaGenerationListeners() {
  const schemaModeSelect = document.getElementById("schemaSourceSchemaMode");
  const tableListFile = document.getElementById("schemaSourceTableListFile");
  
  if (schemaModeSelect) {
    schemaModeSelect.addEventListener("change", function() {
      const singleModeFields = document.getElementById("schemaSourceSingleModeFields");
      const multipleModeFields = document.getElementById("schemaSourceMultipleModeFields");
      
      if (this.value === "single") {
        singleModeFields.classList.remove("hidden");
        multipleModeFields.classList.add("hidden");
      } else if (this.value === "multiple") {
        singleModeFields.classList.add("hidden");
        multipleModeFields.classList.remove("hidden");
      } else {
        singleModeFields.classList.add("hidden");
        multipleModeFields.classList.add("hidden");
      }
    });
  }
  
  if (tableListFile) {
    tableListFile.addEventListener("change", function() {
      const fileName = document.getElementById("schemaSourceTableListFileName");
      if (this.files && this.files[0]) {
        fileName.textContent = `Selected: ${this.files[0].name}`;
        fileName.classList.remove("hidden");
      } else {
        fileName.classList.add("hidden");
      }
    });
  }
}

// Execute Schema Generation operation (handles file upload for multiple mode)
async function executeSchemaGenerationOperation(event) {
  event.preventDefault();
  
  const button = document.getElementById("schema-run-btn");
  const schemaMode = document.getElementById("schemaSourceSchemaMode")?.value;
  const outputFileName = document.getElementById("schemaTargetOutputFileName")?.value?.trim();
  
  // Validate required fields
  if (!schemaMode) {
    addLog("❌ Error: Schema Mode is required");
    alert("Please select a Schema Mode (Single or Multiple)");
    return;
  }
  
  if (!outputFileName) {
    addLog("❌ Error: Output File Name is required");
    alert("Please enter an Output File Name");
    return;
  }
  
  // Validate based on schema mode
  if (schemaMode === "single") {
    const tableName = document.getElementById("schemaSourceTableName")?.value?.trim();
    if (!tableName) {
      addLog("❌ Error: Table Name is required for Single mode");
      alert("Please enter a Table Name");
      return;
    }
  } else if (schemaMode === "multiple") {
    const tableListFile = document.getElementById("schemaSourceTableListFile");
    if (!tableListFile || !tableListFile.files || !tableListFile.files[0]) {
      addLog("❌ Error: Table List File is required for Multiple mode");
      alert("Please upload a Table List File");
      return;
    }
  }
  
  try {
    setButtonLoadingState(button, "Schema Generation", true);
    addLog("🚀 Starting Schema Generation...");
    
    const apiTab = "schema_generation";
    const formData = collectFormData("schema");
    
    // Add schema-specific fields
    formData.schema_mode = schemaMode;
    formData.output_file_name = outputFileName;
    
    let response;
    
    if (schemaMode === "multiple") {
      // Use FormData for file upload
      const tableListFile = document.getElementById("schemaSourceTableListFile");
      const multiFormData = new FormData();
      
      multiFormData.append("tab", apiTab);
      multiFormData.append("schema_mode", schemaMode);
      multiFormData.append("output_file_name", outputFileName);
      multiFormData.append("table_list_file", tableListFile.files[0]);
      
      // Append all other form fields
      Object.keys(formData).forEach((key) => {
        if (formData[key] !== null && formData[key] !== undefined && key !== "schemaSourceTableListFile") {
          multiFormData.append(key, formData[key]);
        }
      });
      
      addLog(`📊 Uploading Table List File: ${tableListFile.files[0].name}`);
      addLog(`📊 Using /api/static/schema/run-with-upload endpoint`);
      
      response = await fetch("/api/static/schema/run-with-upload", {
        method: "POST",
        body: multiFormData,
      });
    } else {
      // Standard JSON request for single mode
      response = await fetch("/api/static/run", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          tab: apiTab,
          params: formData,
        }),
      });
    }
    
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || "Schema Generation operation failed");
    }
    
    const result = await response.json();
    currentRunId = result.run_id;
    executionStatus = "RUNNING";
    
    addLog(`📋 Run ID: ${currentRunId}`);
    
    // Start SSE log streaming with schema-specific handler
    startSchemaGenerationLogStreaming(currentRunId, button);
    
  } catch (error) {
    console.error("Error executing Schema Generation:", error);
    addLog(`❌ Error: ${error.message}`);
    executionStatus = "IDLE";
    setButtonLoadingState(button, "Schema Generation", false);
  }
}

// Start Schema Generation specific log streaming (with download handling)
function startSchemaGenerationLogStreaming(runId, button) {
  if (eventSource) {
    eventSource.close();
  }
  
  eventSource = new EventSource(`/api/static/logs/${runId}`);
  
  eventSource.onmessage = function(event) {
    try {
      const data = JSON.parse(event.data);
      
      switch (data.type) {
        case "log":
          addLog(data.message);
          break;
          
        case "status":
          executionStatus = data.status;
          updateExecutionStatusUI(data.status);
          break;
          
        case "end":
          eventSource.close();
          eventSource = null;
          executionStatus = data.status || "IDLE";
          setButtonLoadingState(button, "Schema Generation", false);
          
          if (data.status === "SUCCESS") {
            addLog("✅ Schema Generation completed successfully!");
            
            // Enable download button
            const downloadSection = document.getElementById("schema-download-section");
            const downloadBtn = document.getElementById("schema-download-btn");
            
            if (downloadSection && downloadBtn) {
              downloadSection.classList.remove("hidden");
              downloadBtn.disabled = false;
              downloadBtn.onclick = function() {
                handleSchemaDownload(runId);
              };
              addLog("📥 Download ready! Click the Download button below the logs.");
            }
          }
          break;
          
        case "error":
          addLog(`❌ ${data.message}`);
          eventSource.close();
          eventSource = null;
          executionStatus = "FAILED";
          setButtonLoadingState(button, "Schema Generation", false);
          break;
      }
    } catch (e) {
      console.error("Error parsing SSE data:", e);
    }
  };
  
  eventSource.onerror = function(error) {
    console.error("SSE error:", error);
    addLog("❌ Connection to server lost");
    eventSource.close();
    eventSource = null;
    executionStatus = "FAILED";
    setButtonLoadingState(button, "Schema Generation", false);
  };
}

// Handle Schema Generation download
function handleSchemaDownload(runId) {
  const outputFileName = document.getElementById("schemaTargetOutputFileName")?.value?.trim() || "schema_output.sql";
  
  addLog(`📥 Downloading ${outputFileName}...`);
  
  const downloadUrl = `/api/static/schema/download/${runId}`;
  
  fetch(downloadUrl)
    .then(response => {
      if (!response.ok) {
        throw new Error("Download failed");
      }
      return response.blob();
    })
    .then(blob => {
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = outputFileName;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      addLog(`✅ Downloaded: ${outputFileName}`);
    })
    .catch(error => {
      console.error("Download error:", error);
      addLog(`❌ Download failed: ${error.message}`);
    });
}

// Generate File Load form - File-to-Database only
function generateFileLoadForm() {
  const container = document.getElementById("fileload-form");
  if (!container) return;

  container.innerHTML = `
        <form class="space-y-6" onsubmit="executeOperation(event, 'File Load')">
            <!-- Header: File to Database Load -->
            <div class="mb-6 p-4 border-2 border-dashed border-fileload/30 rounded-xl bg-gradient-to-br from-card to-fileload/5 shadow-sm">
                <div class="flex items-center gap-2">
                    <span class="text-2xl">📁</span>
                    <span class="text-lg font-semibold text-foreground">File to Database Load</span>
                </div>
                <p class="text-sm text-gray-500 mt-1">Load data from file to target database</p>
            </div>
            
            <div id="fileload-form-fields">
                <div class="grid lg:grid-cols-2 gap-6">
                    ${generateFileLoadSourceFields("fileloadSource")}
                    ${generateFileLoadTargetFields("fileloadTarget")}
                </div>
            </div>
            
            ${generateTimeZoneSection("fileload")}
            
            ${generateUnixServerSection("fileload")}
            
            <div class="execute-section text-center p-6 border-t border-gray-200">
                <!-- File Load Status Display -->
                <div id="fileload-status-container" class="mb-4 hidden">
                    <span id="fileload-status-text" class="text-lg font-semibold"></span>
                </div>
                <button type="submit" id="fileload-run-btn" class="execute-button bg-gradient-to-r from-red-600 to-red-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-red-700 hover:to-red-800 transition-all duration-200 shadow-lg">
                    <span class="execute-icon mr-2">▶️</span>
                    <span id="fileload-btn-text">Execute File Load</span>
                </button>
            </div>
        </form>
    `;

  // Attach event listeners for file fields and target Hive fields
  attachFileLoadSourceListeners();
  attachFileLoadTargetListeners();
  console.log("[FileLoad] Form generated with File-to-Database configuration");
}

// Generate File Load SOURCE fields (file-based, matching Data Comparison file-to-db)
function generateFileLoadSourceFields(prefix) {
  return `
        <div class="source-target-card border-t-4 border-t-fileload shadow-elevated">
             <div class="card-header gradient-header section-header-source">
                 <h3 class="text-fileload flex items-center gap-2">
                     <div class="w-3 h-3 bg-fileload rounded-full"></div>
                     Source Configuration
                 </h3>
                 <p>Configure your source file details</p>
             </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}FileType" class="block text-sm font-medium mb-2">📄 File Type</label>
                    <select id="${prefix}FileType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200">
                        <option value="">Select file type</option>
                        <option value="csv">CSV</option>
                        <option value="json">JSON</option>
                        <option value="xml">XML</option>
                        <option value="parquet">Parquet</option>
                        <option value="excel">Excel</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}LocationType" class="block text-sm font-medium mb-2">📍 Location Type</label>
                    <select id="${prefix}LocationType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200">
                        <option value="">Select location type</option>
                        <option value="unix">File Path (Unix)</option>
                        <option value="hadoop">File Path (Hadoop)</option>
                        <option value="upload">Upload File</option>
                    </select>
                </div>
                
                <div id="${prefix}UnixPathField" class="form-group hidden">
                    <label for="${prefix}UnixPath" class="block text-sm font-medium mb-2">📂 Unix File Path</label>
                    <input type="text" id="${prefix}UnixPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" placeholder="/path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}HadoopPathField" class="form-group hidden">
                    <label for="${prefix}HadoopPath" class="block text-sm font-medium mb-2">📂 Hadoop File Path</label>
                    <input type="text" id="${prefix}HadoopPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" placeholder="hdfs:///path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}FileUploadField" class="form-group hidden">
                    <label for="${prefix}FileUpload" class="block text-sm font-medium mb-2">📁 Upload File</label>
                    <input type="file" id="${prefix}FileUpload" class="form-file w-full p-3 border-2 border-dashed border-gray-300 rounded-lg hover:border-orange-500 cursor-pointer" accept=".csv,.json,.xml,.parquet,.xlsx,.xls">
                    <p id="${prefix}FileUploadName" class="text-sm text-gray-600 mt-1 hidden"></p>
                </div>
                
                <div id="${prefix}DelimiterField" class="form-group hidden">
                    <label for="${prefix}Delimiter" class="block text-sm font-medium mb-2">✂️ Delimiter</label>
                    <select id="${prefix}Delimiter" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200">
                        <option value=",">Comma (,)</option>
                        <option value="|">Pipe (|)</option>
                        <option value="	">Tab</option>
                        <option value=";">Semicolon (;)</option>
                    </select>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}FileSqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}FileSqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" rows="3" placeholder="Enter SQL query to filter file data (optional, default: N/A)"></textarea>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}KeyColumns" class="block text-sm font-medium mb-2">🔑 Key Columns</label>
                    <input type="text" id="${prefix}KeyColumns" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" placeholder="id,customer_id (comma-separated)" maxlength="500">
                </div>
            </div>
        </div>
    `;
}

// Generate File Load TARGET fields (database, matching Data Load target)
function generateFileLoadTargetFields(prefix) {
  // Filter out "File" from database types for target
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-red flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
                    Target Configuration
                </h3>
                <p>Configure your target database connection details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
                    <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
                </div>
                
                ${generatePrimaryKeyDropdown(prefix, "orange-500")}
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
                
                <!-- Load Type (Target only) -->
                <div class="form-group">
                    <label for="${prefix}LoadType" class="block text-sm font-medium mb-2">⚡ Load Type</label>
                    <select id="${prefix}LoadType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:ring-2 focus:ring-orange-200">
                        <option value="">Select load type</option>
                        <option value="overwrite">Overwrite</option>
                        <option value="append">Append</option>
                    </select>
                </div>
                
                <!-- Hive-specific fields (hidden by default) -->
                <div id="${prefix}HiveFields" class="hidden space-y-4 p-4 border-2 border-dashed border-yellow-400 rounded-lg bg-yellow-50">
                    <div class="flex items-center gap-2 mb-2">
                        <span class="text-xl">🐝</span>
                        <span class="text-sm font-semibold text-yellow-700">Hive-Specific Configuration</span>
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}HadoopPath" class="block text-sm font-medium mb-2">📂 Hadoop Path <span class="text-red-500">*</span></label>
                        <input type="text" id="${prefix}HadoopPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="/user/hive/warehouse/..." maxlength="1000">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}PartitionId" class="block text-sm font-medium mb-2">🔖 Partition ID</label>
                        <input type="text" id="${prefix}PartitionId" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="e.g., year=2024/month=01 (leave empty for N/A)" maxlength="500">
                        <p class="text-sm text-gray-500 mt-1">Leave empty to default to "N/A"</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Attach File Load source field listeners (file type, location type, etc.)
function attachFileLoadSourceListeners() {
  const fileTypeSelect = document.getElementById("fileloadSourceFileType");
  const locationTypeSelect = document.getElementById("fileloadSourceLocationType");
  const fileUpload = document.getElementById("fileloadSourceFileUpload");
  
  if (fileTypeSelect) {
    fileTypeSelect.addEventListener("change", function() {
      const delimiterField = document.getElementById("fileloadSourceDelimiterField");
      if (this.value === "csv") {
        delimiterField.classList.remove("hidden");
        console.log("[FileLoad] Delimiter field shown for CSV");
      } else {
        delimiterField.classList.add("hidden");
      }
    });
  }
  
  if (locationTypeSelect) {
    locationTypeSelect.addEventListener("change", function() {
      const unixPathField = document.getElementById("fileloadSourceUnixPathField");
      const hadoopPathField = document.getElementById("fileloadSourceHadoopPathField");
      const uploadField = document.getElementById("fileloadSourceFileUploadField");
      
      // Hide all path fields first
      unixPathField.classList.add("hidden");
      hadoopPathField.classList.add("hidden");
      uploadField.classList.add("hidden");
      
      if (this.value === "unix") {
        unixPathField.classList.remove("hidden");
        console.log("[FileLoad] Unix path field shown");
      } else if (this.value === "hadoop") {
        hadoopPathField.classList.remove("hidden");
        console.log("[FileLoad] Hadoop path field shown");
      } else if (this.value === "upload") {
        uploadField.classList.remove("hidden");
        console.log("[FileLoad] File upload field shown");
      }
    });
  }
  
  if (fileUpload) {
    fileUpload.addEventListener("change", function() {
      const fileName = document.getElementById("fileloadSourceFileUploadName");
      if (this.files && this.files[0]) {
        fileName.textContent = `Selected: ${this.files[0].name}`;
        fileName.classList.remove("hidden");
      } else {
        fileName.classList.add("hidden");
      }
    });
  }
}

// Attach File Load target field listeners (for Hive fields toggle)
function attachFileLoadTargetListeners() {
  const targetTypeSelect = document.getElementById("fileloadTargetType");
  
  if (targetTypeSelect) {
    targetTypeSelect.addEventListener("change", function() {
      const hiveFields = document.getElementById("fileloadTargetHiveFields");
      
      if (this.value === "Hive") {
        if (hiveFields) {
          hiveFields.classList.remove("hidden");
          console.log("[FileLoad] Hive-specific fields shown");
        }
      } else {
        if (hiveFields) {
          hiveFields.classList.add("hidden");
        }
      }
    });
  }
}

// Generate File Download form
function generateFileDownloadForm() {
  const container = document.getElementById("filedownload-form");
  if (!container) return;

  container.innerHTML = `
        <!-- Source Type Selection -->
        <div class="mb-6 p-6 border-2 border-dashed border-primary/30 rounded-xl bg-gradient-to-br from-card to-primary/5 shadow-sm">
            <label class="text-lg font-semibold mb-4 block text-foreground flex items-center gap-2">
                <span class="text-2xl">📥</span> Source Type
            </label>
            <select id="filedownload-source-type-select" class="form-select w-full p-3 h-12 border-2 border-gray-300 rounded-lg text-base font-medium hover-border-primary transition-colors focus:border-primary focus:ring-2 focus:ring-primary/20">
                <option value="">Select source type</option>
                <option value="file">File</option>
                <option value="database">Database</option>
            </select>
        </div>
        
        <form id="filedownload-main-form" class="space-y-6" onsubmit="executeFileDownloadOperation(event)">
            <div id="filedownload-form-fields">
                <p class="text-gray-500 text-center py-8 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">👆 Please select a source type above to continue</p>
            </div>
        </form>
    `;

  // Attach source type selection listener
  const sourceTypeSelect = document.getElementById("filedownload-source-type-select");
  if (sourceTypeSelect) {
    sourceTypeSelect.addEventListener("change", function() {
      handleFileDownloadSourceTypeChange(this.value);
    });
  }
}

// Handle source type change for File Download
function handleFileDownloadSourceTypeChange(sourceType) {
  const formFieldsContainer = document.getElementById("filedownload-form-fields");
  if (!formFieldsContainer) return;

  if (!sourceType) {
    formFieldsContainer.innerHTML = '<p class="text-gray-500 text-center py-8 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">👆 Please select a source type above to continue</p>';
    return;
  }

  let sourceContent = "";
  
  if (sourceType === "file") {
    sourceContent = generateFileDownloadSourceFileFields("filedownloadSource");
  } else if (sourceType === "database") {
    sourceContent = generateFileDownloadSourceDatabaseFields("filedownloadSource");
  }

  formFieldsContainer.innerHTML = `
        <div class="grid lg:grid-cols-2 gap-6">
            ${sourceContent}
            ${generateFileDownloadTargetFields("filedownloadTarget")}
        </div>
        
        ${generateUnixServerSection("filedownload")}
        
        <div class="execute-section text-center p-6 border-t border-gray-200">
            <!-- File Download Status Display -->
            <div id="filedownload-status-container" class="mb-4 hidden">
                <span id="filedownload-status-text" class="text-lg font-semibold"></span>
            </div>
            <button type="submit" id="filedownload-run-btn" class="execute-button bg-gradient-to-r from-teal-600 to-teal-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-teal-700 hover:to-teal-800 transition-all duration-200 shadow-lg">
                <span class="execute-icon mr-2">▶️</span>
                <span id="filedownload-btn-text">Execute File Download</span>
            </button>
        </div>
    `;

  // Attach appropriate listeners based on source type
  if (sourceType === "file") {
    attachFileDownloadSourceFileListeners();
  } else if (sourceType === "database") {
    attachFormListeners("filedownload");
  }
  
  // Attach target file type listener for delimiter toggle
  attachFileDownloadTargetListeners();
}

// Generate FILE source fields for File Download (matching Data Comparison file behavior)
function generateFileDownloadSourceFileFields(prefix) {
   return `
         <div class="source-target-card border-t-4 border-t-filedownload shadow-elevated">
             <div class="card-header gradient-header section-header-source">
                 <h3 class="text-filedownload-dark flex items-center gap-2">
                     <div class="w-3 h-3 bg-filedownload rounded-full"></div>
                     Source Configuration
                 </h3>
                 <p>Configure your source file details</p>
             </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}FileType" class="block text-sm font-medium mb-2">📄 File Type</label>
                    <select id="${prefix}FileType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200">
                        <option value="">Select file type</option>
                        <option value="CSV">CSV</option>
                        <option value="TSV">TSV</option>
                        <option value="Excel">Excel</option>
                        <option value="JSON">JSON</option>
                        <option value="Parquet">Parquet</option>
                        <option value="XML">XML</option>
                        <option value="Dat">Dat or txt file with Delimiter</option>
                    </select>
                </div>
                
                <div id="${prefix}DelimiterField" class="form-group hidden">
                    <label for="${prefix}Delimiter" class="block text-sm font-medium mb-2">📝 Delimiter</label>
                    <input type="text" id="${prefix}Delimiter" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter delimiter (e.g., comma, pipe, tab)" maxlength="10">
                </div>
                
                <div class="form-group">
                    <label for="${prefix}LocationType" class="block text-sm font-medium mb-2">📍 Location Type</label>
                    <select id="${prefix}LocationType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200">
                        <option value="">Select location type</option>
                        <option value="unix">File Path (Unix)</option>
                        <option value="hadoop">File Path (Hadoop)</option>
                        <option value="upload">Upload from local</option>
                    </select>
                </div>
                
                <div id="${prefix}UnixPathField" class="form-group hidden">
                    <label for="${prefix}UnixPath" class="block text-sm font-medium mb-2">📂 Unix File Path</label>
                    <input type="text" id="${prefix}UnixPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="/path/to/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}HadoopPathField" class="form-group hidden">
                    <label for="${prefix}HadoopPath" class="block text-sm font-medium mb-2">📂 Hadoop File Path</label>
                    <input type="text" id="${prefix}HadoopPath" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="/user/hadoop/file.csv" maxlength="1000">
                </div>
                
                <div id="${prefix}LocalUploadField" class="form-group hidden">
                    <label for="${prefix}LocalFile" class="block text-sm font-medium mb-2">📤 Upload Local File <span class="text-red-500">*</span></label>
                    <input type="file" id="${prefix}LocalFile" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-semibold file:bg-teal-50 file:text-teal-700 hover:file:bg-teal-100">
                    <p class="text-sm text-gray-500 mt-1">Select a file from your local system to upload</p>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}KeyColumns" class="block text-sm font-medium mb-2">🔑 Key Columns</label>
                    <input type="text" id="${prefix}KeyColumns" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter key column(s), e.g., ID or ID,COUNTRY_CODE" maxlength="500">
                    <p class="text-sm text-gray-500 mt-1">For composite keys, enter comma-separated column names</p>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
            </div>
        </div>
    `;
}

// Generate DATABASE source fields for File Download (matching Data Comparison DB behavior)
function generateFileDownloadSourceDatabaseFields(prefix) {
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");
  
   return `
         <div class="source-target-card border-t-4 border-t-filedownload shadow-elevated">
             <div class="card-header gradient-header section-header-source">
                 <h3 class="text-filedownload-dark flex items-center gap-2">
                     <div class="w-3 h-3 bg-filedownload rounded-full"></div>
                     Source Configuration
                 </h3>
                 <p>Configure your source database connection details</p>
             </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}Type" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
                    <select id="${prefix}Type" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200">
                        <option value="">Select database type</option>
                        ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
                    </select>
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Host" class="block text-sm font-medium mb-2">🌐 Host</label>
                        <input type="text" id="${prefix}Host" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="localhost" maxlength="1500">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Port" class="block text-sm font-medium mb-2">🔌 Port</label>
                        <input type="text" id="${prefix}Port" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="5432" maxlength="10">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}Database" class="block text-sm font-medium mb-2">💾 Database Name</label>
                    <input type="text" id="${prefix}Database" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter database name" maxlength="255">
                </div>
                
                <div class="grid grid-cols-2 gap-4">
                    <div class="form-group">
                        <label for="${prefix}Username" class="block text-sm font-medium mb-2">👤 Username</label>
                        <input type="text" id="${prefix}Username" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter username" maxlength="100">
                    </div>
                    
                    <div class="form-group">
                        <label for="${prefix}Password" class="block text-sm font-medium mb-2">🔒 Password</label>
                        <input type="password" id="${prefix}Password" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter password" maxlength="100">
                    </div>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}TableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
                    <input type="text" id="${prefix}TableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" placeholder="Enter table name" maxlength="255">
                </div>
                
                ${generatePrimaryKeyDropdown(prefix, "teal-500")}
                
                <div class="form-group">
                    <label for="${prefix}SqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
                    <textarea id="${prefix}SqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-teal-500 focus:ring-2 focus:ring-teal-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
                </div>
            </div>
        </div>
    `;
}

// Generate TARGET file fields for File Download (always file output)
function generateFileDownloadTargetFields(prefix) {
  return `
        <div class="source-target-card border-t-4 border-t-wellsfargo-gold shadow-elevated">
            <div class="card-header gradient-header">
                <h3 class="text-wellsfargo-gold flex items-center gap-2">
                    <div class="w-3 h-3 bg-wellsfargo-gold rounded-full"></div>
                    Target File Configuration
                </h3>
                <p>Configure your output file details</p>
            </div>
            <div class="config-content p-6 space-y-4">
                <div class="form-group">
                    <label for="${prefix}FileType" class="block text-sm font-medium mb-2">📄 Target File Type</label>
                    <select id="${prefix}FileType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200">
                        <option value="">Select file type</option>
                        <option value="CSV">CSV</option>
                        <option value="TSV">TSV</option>
                        <option value="JSON">JSON</option>
                        <option value="Parquet">Parquet</option>
                        <option value="XML">XML</option>
                        <option value="Excel">Excel (XLSX)</option>
                    </select>
                </div>
                
                <div id="${prefix}DelimiterField" class="form-group hidden">
                    <label for="${prefix}Delimiter" class="block text-sm font-medium mb-2">📝 Delimiter <span class="text-red-500">*</span></label>
                    <input type="text" id="${prefix}Delimiter" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="Enter delimiter (e.g., , or | or \\t)" value="," maxlength="10">
                    <p class="text-sm text-gray-500 mt-1">Required for CSV/TSV formats</p>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}FileName" class="block text-sm font-medium mb-2">📁 Target File Name <span class="text-red-500">*</span></label>
                    <input type="text" id="${prefix}FileName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="e.g., output_data.csv" maxlength="255" required>
                    <p class="text-sm text-gray-500 mt-1">This will be the downloaded file name</p>
                </div>
                
                <div class="form-group">
                    <label for="${prefix}NumberOfRows" class="block text-sm font-medium mb-2">🔢 Number of Rows</label>
                    <input type="number" id="${prefix}NumberOfRows" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-yellow-500 focus:ring-2 focus:ring-yellow-200" placeholder="Enter number of rows (max 100000)" min="1" max="100000">
                    <p class="text-sm text-gray-500 mt-1">Maximum allowed: 100,000 rows. Leave empty for default.</p>
                </div>
            </div>
        </div>
    `;
}

// Attach listeners for File Download source file fields
function attachFileDownloadSourceFileListeners() {
  const fileTypeSelect = document.getElementById("filedownloadSourceFileType");
  const locationTypeSelect = document.getElementById("filedownloadSourceLocationType");
  
  if (fileTypeSelect) {
    fileTypeSelect.addEventListener("change", function() {
      const delimiterField = document.getElementById("filedownloadSourceDelimiterField");
      if (this.value === "CSV" || this.value === "TSV" || this.value === "Dat") {
        if (delimiterField) delimiterField.classList.remove("hidden");
      } else {
        if (delimiterField) delimiterField.classList.add("hidden");
      }
    });
  }
  
  if (locationTypeSelect) {
    locationTypeSelect.addEventListener("change", function() {
      const unixPathField = document.getElementById("filedownloadSourceUnixPathField");
      const hadoopPathField = document.getElementById("filedownloadSourceHadoopPathField");
      const localUploadField = document.getElementById("filedownloadSourceLocalUploadField");
      
      // Hide all path fields first
      if (unixPathField) unixPathField.classList.add("hidden");
      if (hadoopPathField) hadoopPathField.classList.add("hidden");
      if (localUploadField) localUploadField.classList.add("hidden");
      
      if (this.value === "unix") {
        if (unixPathField) unixPathField.classList.remove("hidden");
      } else if (this.value === "hadoop") {
        if (hadoopPathField) hadoopPathField.classList.remove("hidden");
      } else if (this.value === "upload") {
        if (localUploadField) localUploadField.classList.remove("hidden");
      }
    });
  }
}

// Attach listeners for File Download target fields
function attachFileDownloadTargetListeners() {
  const targetFileTypeSelect = document.getElementById("filedownloadTargetFileType");
  
  if (targetFileTypeSelect) {
    targetFileTypeSelect.addEventListener("change", function() {
      const delimiterField = document.getElementById("filedownloadTargetDelimiterField");
      if (this.value === "CSV" || this.value === "TSV") {
        if (delimiterField) delimiterField.classList.remove("hidden");
      } else {
        if (delimiterField) delimiterField.classList.add("hidden");
      }
    });
  }
}

// File Download execution state
let fileDownloadRunId = null;
let fileDownloadStatus = "IDLE";
let fileDownloadUrl = "";

// Execute File Download operation
async function executeFileDownloadOperation(event) {
  if (event) {
    event.preventDefault();
  }

  console.log("[FileDownload] Executing File Download operation");
  
  const button = document.getElementById("filedownload-run-btn");
  const downloadSection = document.getElementById("filedownload-download-section");
  const downloadBtn = document.getElementById("filedownload-download-btn");
  
  // Hide download button at start
  if (downloadSection) downloadSection.classList.add("hidden");
  if (downloadBtn) downloadBtn.disabled = true;

  // Check if already running
  if (fileDownloadStatus === "RUNNING") {
    addLog("⚠️ File Download operation is already running. Please wait.");
    return;
  }

  // Validate required fields
  const sourceType = document.getElementById("filedownload-source-type-select")?.value;
  if (!sourceType) {
    addLog("❌ Please select a source type (File or Database)");
    return;
  }
  
  // Validate Target File Name (mandatory)
  const targetFileName = document.getElementById("filedownloadTargetFileName")?.value?.trim();
  if (!targetFileName) {
    addLog("❌ Target File Name is required");
    return;
  }
  
  // Validate Number of Rows (max 100000)
  const numberOfRowsInput = document.getElementById("filedownloadTargetNumberOfRows");
  const numberOfRows = numberOfRowsInput?.value ? parseInt(numberOfRowsInput.value, 10) : null;
  
  if (numberOfRows !== null && numberOfRows > 100000) {
    addLog("❌ Number of Rows cannot exceed 100,000");
    return;
  }
  
  if (numberOfRows !== null && (isNaN(numberOfRows) || numberOfRows < 1)) {
    addLog("❌ Number of Rows must be a positive number");
    return;
  }
  
  // Validate local file upload if location type is "upload"
  const locationType = document.getElementById("filedownloadSourceLocationType")?.value;
  if (sourceType === "file" && locationType === "upload") {
    const localFileInput = document.getElementById("filedownloadSourceLocalFile");
    if (!localFileInput || !localFileInput.files || !localFileInput.files[0]) {
      addLog("❌ Please select a local file to upload");
      return;
    }
  }

  // Validate User Email
  const userEmail = document.getElementById("filedownloadUserEmail")?.value?.trim();
  if (!isValidEmail(userEmail)) {
    if (!userEmail) {
      addLog("❌ User Email is required for execution");
    } else {
      addLog("❌ Please enter a valid email address");
    }
    return;
  }

  // Show loading state on button
  setButtonLoadingState(button, "File Download", true);
  fileDownloadStatus = "RUNNING";

  try {
    // Collect form data
    const formData = collectFileDownloadFormData(sourceType);
    
    // Add required fields
    formData.source_type = sourceType;
    formData.target_file_name = targetFileName;
    if (numberOfRows !== null) {
      formData.number_of_rows = numberOfRows;
    }

    addLog(`🚀 Starting File Download...`);
    addLog(`📁 Source Type: ${sourceType}`);
    addLog(`📄 Target File Name: ${targetFileName}`);
    if (numberOfRows) addLog(`🔢 Number of Rows: ${numberOfRows}`);

    let response;
    
    // Check if local file upload is needed
    const locationType = document.getElementById("filedownloadSourceLocationType")?.value;
    if (sourceType === "file" && locationType === "upload") {
      // Use multipart/form-data for file upload
      const localFileInput = document.getElementById("filedownloadSourceLocalFile");
      const localFile = localFileInput.files[0];
      
      addLog(`📤 Uploading local file: ${localFile.name} (${(localFile.size / 1024).toFixed(2)} KB)`);
      
      const uploadFormData = new FormData();
      uploadFormData.append("tab", "file_download");
      uploadFormData.append("source_type", sourceType);
      uploadFormData.append("source_location_type", "upload");
      uploadFormData.append("source_local_file", localFile);
      uploadFormData.append("target_file_name", targetFileName);
      if (numberOfRows !== null) {
        uploadFormData.append("number_of_rows", numberOfRows);
      }
      
      // Append all other form fields
      Object.keys(formData).forEach((key) => {
        if (formData[key] !== null && formData[key] !== undefined) {
          uploadFormData.append(key, formData[key]);
        }
      });
      
      response = await fetch("/api/static/filedownload/run-with-upload", {
        method: "POST",
        body: uploadFormData,
      });
    } else {
      // Standard JSON request
      response = await fetch("/api/static/run", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          tab: "file_download",
          params: formData,
        }),
      });
    }

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || "File Download operation failed");
    }

    const result = await response.json();
    fileDownloadRunId = result.run_id;

    addLog(`📋 Run ID: ${fileDownloadRunId}`);

    // Start SSE log streaming for File Download
    startFileDownloadLogStreaming(fileDownloadRunId, button, targetFileName);
  } catch (error) {
    console.error("Error executing File Download:", error);
    addLog(`❌ Error: ${error.message}`);
    fileDownloadStatus = "IDLE";
    setButtonLoadingState(button, "File Download", false);
  }
}

// Start SSE log streaming for File Download
function startFileDownloadLogStreaming(runId, button, targetFileName) {
  const eventSrc = new EventSource(`/api/static/logs/${runId}`);
  
  eventSrc.onmessage = function(event) {
    try {
      const data = JSON.parse(event.data);
      
      switch (data.type) {
        case "log":
          addLog(data.message);
          break;
          
        case "status":
          fileDownloadStatus = data.status;
          break;
          
        case "end":
          eventSrc.close();
          fileDownloadStatus = data.status || "IDLE";
          setButtonLoadingState(button, "File Download", false);
          
          if (data.status === "SUCCESS") {
            addLog("✅ File Download completed successfully!");
            
            // Show download button (in the log section, after the log box)
            const downloadSection = document.getElementById("filedownload-download-section");
            const downloadBtn = document.getElementById("filedownload-download-btn");
            const downloadBtnText = document.getElementById("filedownload-download-btn-text");
            
            if (downloadSection) downloadSection.classList.remove("hidden");
            if (downloadBtn) {
              downloadBtn.disabled = false;
              downloadBtn.onclick = function() {
                handleFileDownloadDownload(runId, targetFileName);
              };
            }
            if (downloadBtnText) downloadBtnText.textContent = `Download ${targetFileName}`;
            
            // Store download URL
            fileDownloadUrl = `/api/static/filedownload/download/${runId}`;
            addLog(`📥 Click "Download ${targetFileName}" button below the logs to save the file.`);
          }
          break;
          
        case "error":
          addLog(`❌ ${data.message}`);
          eventSrc.close();
          fileDownloadStatus = "FAILED";
          setButtonLoadingState(button, "File Download", false);
          break;
      }
    } catch (e) {
      console.error("Error parsing SSE data:", e);
    }
  };
  
  eventSrc.onerror = function(error) {
    console.error("SSE error:", error);
    addLog("❌ Connection to server lost");
    eventSrc.close();
    fileDownloadStatus = "FAILED";
    setButtonLoadingState(button, "File Download", false);
  };
}

// Collect form data for File Download
function collectFileDownloadFormData(sourceType) {
  const data = {};
  
  // Source fields based on source type
  if (sourceType === "file") {
    data.filedownloadSourceFileType = document.getElementById("filedownloadSourceFileType")?.value || "";
    data.filedownloadSourceDelimiter = document.getElementById("filedownloadSourceDelimiter")?.value || "";
    data.filedownloadSourceLocationType = document.getElementById("filedownloadSourceLocationType")?.value || "";
    data.filedownloadSourceUnixPath = document.getElementById("filedownloadSourceUnixPath")?.value || "";
    data.filedownloadSourceHadoopPath = document.getElementById("filedownloadSourceHadoopPath")?.value || "";
    data.filedownloadSourceKeyColumns = document.getElementById("filedownloadSourceKeyColumns")?.value || "";
    data.filedownloadSourceSqlQuery = document.getElementById("filedownloadSourceSqlQuery")?.value || "";
    // Note: Local file is handled separately via FormData in executeFileDownloadOperation
  } else if (sourceType === "database") {
    data.filedownloadSourceType = document.getElementById("filedownloadSourceType")?.value || "";
    data.filedownloadSourceHost = document.getElementById("filedownloadSourceHost")?.value || "";
    data.filedownloadSourcePort = document.getElementById("filedownloadSourcePort")?.value || "";
    data.filedownloadSourceDatabase = document.getElementById("filedownloadSourceDatabase")?.value || "";
    data.filedownloadSourceUsername = document.getElementById("filedownloadSourceUsername")?.value || "";
    data.filedownloadSourcePassword = document.getElementById("filedownloadSourcePassword")?.value || "";
    data.filedownloadSourceTableName = document.getElementById("filedownloadSourceTableName")?.value || "";
    data.filedownloadSourcePrimaryKey = resolvePrimaryKeyValue("filedownloadSource");
    data.filedownloadSourceSqlQuery = document.getElementById("filedownloadSourceSqlQuery")?.value || "";
  }
  
  // Target file fields
  data.filedownloadTargetFileType = document.getElementById("filedownloadTargetFileType")?.value || "";
  data.filedownloadTargetDelimiter = document.getElementById("filedownloadTargetDelimiter")?.value || "";
  data.filedownloadTargetFileName = document.getElementById("filedownloadTargetFileName")?.value || "";
  data.filedownloadTargetNumberOfRows = document.getElementById("filedownloadTargetNumberOfRows")?.value || "";
  
  // Unix server fields
  data.filedownloadUnixHost = document.getElementById("filedownloadUnixHost")?.value || "";
  data.filedownloadUserEmail = document.getElementById("filedownloadUserEmail")?.value || "";
  data.filedownloadBatchId = document.getElementById("filedownloadBatchId")?.value || "";
  data.filedownloadUnixUsername = document.getElementById("filedownloadUnixUsername")?.value || "";
  data.filedownloadUnixPassword = document.getElementById("filedownloadUnixPassword")?.value || "";
  
  return data;
}

// Handle File Download download button click
function handleFileDownloadDownload(runId, targetFileName) {
  if (!runId) {
    addLog("❌ No file available to download");
    return;
  }
  
  try {
    addLog(`📥 Downloading ${targetFileName}...`);
    
    // Create a temporary anchor element for download
    const link = document.createElement("a");
    link.href = `/api/static/filedownload/download/${runId}`;
    link.download = targetFileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    addLog(`✅ Download started for ${targetFileName}`);
  } catch (error) {
    console.error("Error downloading file:", error);
    addLog(`❌ Error downloading file: ${error.message}`);
  }
}

// Attach form event listeners
function attachFormListeners(prefix) {
  // Database type change listeners
  const sourceTypeSelect = document.getElementById(prefix + "SourceType");
  const targetTypeSelect = document.getElementById(prefix + "TargetType");

  if (sourceTypeSelect) {
    sourceTypeSelect.addEventListener("change", function () {
      updateDropdownOptions(prefix + "Source", this.value);
    });
  }

  if (targetTypeSelect) {
    targetTypeSelect.addEventListener("change", function () {
      updateDropdownOptions(prefix + "Target", this.value);
    });
  }

  // Primary key selection listeners
  const sourcePkSelect = document.getElementById(prefix + "SourcePrimaryKey");
  const targetPkSelect = document.getElementById(prefix + "TargetPrimaryKey");

  if (sourcePkSelect) {
    sourcePkSelect.addEventListener("change", function () {
      if (this.value === "manual") {
        handlePrimaryKeySelection(prefix + "Source");
      }
    });
  }

  if (targetPkSelect) {
    targetPkSelect.addEventListener("change", function () {
      if (this.value === "manual") {
        handlePrimaryKeySelection(prefix + "Target");
      }
    });
  }
}

// ============================================================
// STATIC EXECUTION FLOW - SSE BASED
// ============================================================

// Helper function to validate email format
function isValidEmail(email) {
  // Handle null, undefined, or non-string values
  if (email === null || email === undefined) return false;
  if (typeof email !== 'string') return false;
  
  // Trim whitespace
  email = email.trim();
  if (email === '') return false;
  
  // Use a simple regex pattern for email validation
  // This pattern checks for: something@something.something
  const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailPattern.test(email);
}

// Execute operation - Main entry point
async function executeOperation(event, operationType) {
  if (event) {
    event.preventDefault();
  }

  console.log(
    `Executing ${operationType} with table mode: ${currentTableMode}`
  );

  // Get the execute button - for Data Load, try specific ID first
  const form = event ? event.target : null;
  let button = null;
  
  if (operationType === "Data Load") {
    // For Data Load, use the specific button ID to ensure isolation
    button = document.getElementById("dataload-run-btn");
    console.log("[DataLoad] Looking for button #dataload-run-btn, found:", !!button);
  }
  
  // Fallback to form's execute button if specific button not found
  if (!button && form) {
    button = form.querySelector(".execute-button");
  }

  // Check if already running
  if (executionStatus === "RUNNING") {
    addLog("⚠️ An operation is already running. Please wait or stop it first.");
    return;
  }

  // Reset download button state when starting a new comparison
  if (operationType === "Data Comparison") {
    resetDownloadButton();
  }

  // Show loading state on button
  console.log(`[${operationType}] Setting button loading state, button found: ${!!button}`);
  setButtonLoadingState(button, operationType, true);

  try {
    // Get the API tab name
    const apiTab = TAB_API_MAP[currentTab];
    if (!apiTab) {
      throw new Error(`Unknown tab: ${currentTab}`);
    }

    // Collect form data based on current tab
    const formData = collectFormData(currentTab);
    
    // Validate User Email
    const emailKey = `${currentTab}UserEmail`;
    const userEmail = formData[emailKey];
    if (!isValidEmail(userEmail)) {
      if (!userEmail || userEmail.trim() === '') {
        throw new Error("User Email is required for execution");
      } else {
        throw new Error("Please enter a valid email address");
      }
    }

    // Add table mode to form data
    formData.tableMode = currentTableMode;

    // Add active tab identifier for Unix script
    formData.active_tab = apiTab;
    console.log(`Active tab passed to payload: ${apiTab}`);

    // Add comparison type if applicable
    const comparisonTypeSelect = document.getElementById(
      "comparison-type-select"
    );
    if (comparisonTypeSelect) {
      formData.comparisonType = comparisonTypeSelect.value;
    }

    addLog(`🚀 Starting ${operationType}...`);

    let response;

    // Check if this is Multi-Table mode with Excel file for Data Comparison
    if (
      apiTab === "data_comparison" &&
      currentTableMode === "multi" &&
      formData.comparisonType === "database-to-database"
    ) {
      // Use multipart/form-data for Excel file upload
      const sourceExcelInput = document.getElementById("sourceExcelFile");
      const targetExcelInput = document.getElementById("targetExcelFile");
      
      // Validate both files are provided
      if (!sourceExcelInput || !sourceExcelInput.files || !sourceExcelInput.files[0]) {
        throw new Error("Please select a Source Excel file for Multiple Tables mode");
      }
      if (!targetExcelInput || !targetExcelInput.files || !targetExcelInput.files[0]) {
        throw new Error("Please select a Target Excel file for Multiple Tables mode");
      }

      const multiFormData = new FormData();
      multiFormData.append("tab", apiTab);
      multiFormData.append("comparison_type", "db_to_db");
      multiFormData.append("mode", "multiple_tables");
      multiFormData.append("source_excel_file", sourceExcelInput.files[0]);
      multiFormData.append("target_excel_file", targetExcelInput.files[0]);

      // Append all other form fields
      Object.keys(formData).forEach((key) => {
        if (formData[key] !== null && formData[key] !== undefined) {
          multiFormData.append(key, formData[key]);
        }
      });

      addLog(`📊 Uploading Source Excel: ${sourceExcelInput.files[0].name}`);
      addLog(`📊 Uploading Target Excel: ${targetExcelInput.files[0].name}`);
      addLog(`📊 Using /api/static/comparison/run-multi endpoint for multi-table mode`);

      response = await fetch("/api/static/comparison/run-multi", {
        method: "POST",
        body: multiFormData,
      });
    } else if (apiTab === "file_load") {
      // Check if File Load tab has local file upload selected
      const fileloadLocationType = document.getElementById("fileloadSourceLocationType")?.value;
      
      if (fileloadLocationType === "upload") {
        // Use multipart/form-data for local file upload
        const fileUploadInput = document.getElementById("fileloadSourceFileUpload");
        
        // Validate file is selected
        if (!fileUploadInput || !fileUploadInput.files || !fileUploadInput.files[0]) {
          throw new Error("Please select a file to upload for File Load operation");
        }
        
        const uploadedFile = fileUploadInput.files[0];
        
        addLog(`📤 Uploading local file: ${uploadedFile.name} (${(uploadedFile.size / 1024).toFixed(2)} KB)`);
        
        const uploadFormData = new FormData();
        uploadFormData.append("tab", "file_load");
        uploadFormData.append("source_local_file", uploadedFile);
        
        // Append all other form fields
        Object.keys(formData).forEach((key) => {
          if (formData[key] !== null && formData[key] !== undefined) {
            uploadFormData.append(key, formData[key]);
          }
        });
        
        addLog(`📤 Using /api/static/fileload/run-with-upload endpoint`);
        
        response = await fetch("/api/static/fileload/run-with-upload", {
          method: "POST",
          body: uploadFormData,
        });
      } else {
        // Standard JSON request for unix/hadoop path
        response = await fetch("/api/static/run", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            tab: apiTab,
            params: formData,
          }),
        });
      }
    } else {
      // Standard JSON request for single table mode
      response = await fetch("/api/static/run", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          tab: apiTab,
          params: formData,
        }),
      });
    }

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || `${operationType} operation failed`);
    }

    const result = await response.json();
    currentRunId = result.run_id;
    executionStatus = "RUNNING";
    
    // Data Load specific state update
    if (apiTab === "data_load") {
      dataLoadRunStatus = "RUNNING";
      console.log("[DataLoad] Status changed to RUNNING → Waiting for DataLoad");
    }

    addLog(`📋 Run ID: ${currentRunId}`);

    // Start SSE log streaming
    startLogStreaming(currentRunId, button, operationType);
  } catch (error) {
    console.error("Error executing operation:", error);
    addLog(`❌ Error: ${error.message}`);
    executionStatus = "IDLE";
    setButtonLoadingState(button, operationType, false);
  }
}

// Start SSE log streaming
function startLogStreaming(runId, button, operationType) {
  // Close existing EventSource if any
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource(`/api/static/logs/${runId}`);

  eventSource.onmessage = function (event) {
    try {
      const data = JSON.parse(event.data);

      switch (data.type) {
        case "log":
          addLog(data.message);
          break;

        case "status":
          executionStatus = data.status;
          updateExecutionStatusUI(data.status);
          break;

        case "end":
          eventSource.close();
          eventSource = null;
          executionStatus = data.status || "IDLE";
          setButtonLoadingState(button, operationType, false);

          if (data.status === "SUCCESS") {
            if (operationType === "Data Comparison") {
              jobComplete = true;
              
              // Check if download is available (single_table mode)
              if (data.download_available && data.download_url) {
                reportUrl = data.download_url;
                enableDownloadButton();
                addLog(`📥 Download ready: ${data.download_filename || 'output.csv'}`);
                addLog("✅ Comparison complete! Click 'Download Comparison Report' to save the file.");
              } else {
                reportUrl = `/reports/comparison_${runId}.csv`;
                enableDownloadButton();
              }
            } else if (operationType === "Data Load") {
              // Data Load specific success handling
              dataLoadRunStatus = "SUCCESS";
              dataLoadButtonLabel = "Execute Data Load";
              console.log("[DataLoad] Status SUCCESS → Data load completed");
              addLog("✅ Data load completed");
              updateDataLoadStatusUI("Data load completed");
            }
          } else if (data.status === "FAILED" && operationType === "Data Load") {
            dataLoadRunStatus = "FAILED";
            dataLoadButtonLabel = "Execute Data Load";
            console.log("[DataLoad] Status FAILED");
          } else if (data.status === "STOPPED" && operationType === "Data Load") {
            dataLoadRunStatus = "STOPPED";
            dataLoadButtonLabel = "Execute Data Load";
            console.log("[DataLoad] Status STOPPED");
          }
          break;

        case "error":
          addLog(`❌ ${data.message}`);
          eventSource.close();
          eventSource = null;
          executionStatus = "FAILED";
          setButtonLoadingState(button, operationType, false);
          break;
      }
    } catch (e) {
      console.error("Error parsing SSE data:", e);
    }
  };

  eventSource.onerror = function (error) {
    console.error("SSE error:", error);
    addLog("❌ Connection to server lost");
    eventSource.close();
    eventSource = null;
    executionStatus = "FAILED";
    setButtonLoadingState(button, operationType, false);
  };
}

// Update execution status in UI
function updateExecutionStatusUI(status) {
  const statusElement = document.getElementById("execution-status");
  if (statusElement) {
    statusElement.textContent = status;
    statusElement.className = `execution-status status-${status.toLowerCase()}`;
  }
  updateLogStatusRing(status);
}

// Update the log header status ring indicator
function updateLogStatusRing(status) {
  const ring = document.getElementById("log-status-ring");
  if (!ring) return;
  
  // Remove any existing animation
  ring.style.animation = 'none';
  
  switch (status) {
    case 'RUNNING':
      ring.style.borderColor = '#22c55e';
      ring.style.borderTopColor = 'transparent';
      ring.style.animation = 'logRingSpin 0.8s linear infinite';
      break;
    case 'SUCCESS':
      ring.style.borderColor = '#22c55e';
      ring.style.borderTopColor = '#22c55e';
      ring.style.animation = 'none';
      break;
    case 'FAILED':
      ring.style.borderColor = '#ef4444';
      ring.style.borderTopColor = '#ef4444';
      ring.style.animation = 'none';
      break;
    default: // IDLE, STOPPED
      ring.style.borderColor = '#9ca3af';
      ring.style.borderTopColor = '#9ca3af';
      ring.style.animation = 'none';
      break;
  }
}

// Set button loading state
function setButtonLoadingState(button, operationType, isLoading) {
  if (!button) return;

  if (isLoading) {
    button.disabled = true;
    
    // Data Load specific button label - use dedicated element
    if (operationType === "Data Load") {
      dataLoadRunStatus = "RUNNING";
      dataLoadButtonLabel = "Waiting for DataLoad...";
      
      // Update Data Load specific button text element
      const dataLoadBtnText = document.getElementById("dataload-btn-text");
      if (dataLoadBtnText) {
        dataLoadBtnText.textContent = "Waiting for DataLoad...";
      }
      button.innerHTML = `<span class="loading-spinner mr-2">⟳</span><span id="dataload-btn-text">Waiting for DataLoad...</span>`;
      console.log("[DataLoad] Button updated → Waiting for DataLoad (element: #dataload-run-btn)");
      
      // Hide success status when starting new run
      const statusContainer = document.getElementById("dataload-status-container");
      if (statusContainer) {
        statusContainer.classList.add("hidden");
      }
    } else {
      button.innerHTML = `<span class="loading-spinner mr-2">⟳</span>Waiting for ${operationType}...`;
    }
    button.classList.add("loading");
  } else {
    button.disabled = false;
    
    // Data Load specific button reset
    if (operationType === "Data Load") {
      dataLoadButtonLabel = "Execute Data Load";
      button.innerHTML = `<span class="execute-icon mr-2">▶️</span><span id="dataload-btn-text">Execute Data Load</span>`;
      console.log("[DataLoad] Button reset → Execute Data Load");
    } else {
      button.innerHTML = `<span class="execute-icon mr-2">▶️</span>Execute ${operationType}`;
    }
    button.classList.remove("loading");
  }
}

// Update Data Load specific status in UI
function updateDataLoadStatusUI(statusText) {
  console.log("[DataLoad] Updating status UI → " + statusText);
  
  // Find status container and text element specific to Data Load tab
  const statusContainer = document.getElementById("dataload-status-container");
  const statusElement = document.getElementById("dataload-status-text");
  
  if (statusContainer && statusElement) {
    statusContainer.classList.remove("hidden");
    statusElement.textContent = statusText;
    statusElement.className = "dataload-status-success text-green-600 font-semibold";
    console.log("[DataLoad] Updated #dataload-status-text → " + statusText);
  } else {
    console.log("[DataLoad] Status elements not found: container=" + !!statusContainer + ", text=" + !!statusElement);
  }
}

// Stop current execution
async function stopExecution() {
  if (!currentRunId || executionStatus !== "RUNNING") {
    addLog("⚠️ No running operation to stop");
    return;
  }

  try {
    const response = await fetch(`/api/static/stop/${currentRunId}`, {
      method: "POST",
    });

    if (response.ok) {
      addLog("⛔ Stop request sent");
    } else {
      const errorData = await response.json();
      addLog(`❌ Failed to stop: ${errorData.error}`);
    }
  } catch (error) {
    console.error("Error stopping execution:", error);
    addLog(`❌ Error stopping execution: ${error.message}`);
  }
}

// Collect form data for current tab
function collectFormData(tabPrefix) {
  const data = {};

  // Get all form inputs in the current tab
  const tabContent = document.getElementById(tabPrefix);
  if (!tabContent) return data;

  const inputs = tabContent.querySelectorAll("input, select, textarea");
  inputs.forEach((input) => {
    if (input.id) {
      if (input.type === "file") {
        // For file inputs, we'll need to handle file upload separately
        // For now, just store the file name
        data[input.id] = input.files[0] ? input.files[0].name : null;
      } else if (input.type === "radio") {
        if (input.checked) {
          data[input.name] = input.value;
        }
      } else if (input.type === "password") {
        // Include password fields but mark them
        data[input.id] = input.value;
      } else {
        data[input.id] = input.value;
      }
    }
  });

  // Resolve PK dropdown values: if "manual", use the manual input value
  Object.keys(data).forEach((key) => {
    if (key.endsWith("PrimaryKey") && data[key] === "manual") {
      const manualVal = data[key + "Manual"];
      data[key] = manualVal ? manualVal.trim() : "";
    }
  });

  // Add key columns if they exist
  const comparisonType = document.getElementById("comparison-type-select");
  if (
    comparisonType &&
    (comparisonType.value === "file-to-file" ||
      comparisonType.value === "file-to-database" ||
      comparisonType.value === "database-to-file")
  ) {
    const sourceKeyColumns = document.getElementById(
      "comparisonSourceKeyColumns"
    );
    const targetKeyColumns = document.getElementById(
      "comparisonTargetKeyColumns"
    );
    if (sourceKeyColumns) data["keyColumns"] = sourceKeyColumns.value;
    if (targetKeyColumns) data["keyColumns"] = targetKeyColumns.value;
    
    // Handle location type mapping to unix_path or hadoop_path
    const sourceLocationType = document.getElementById("comparisonSourceLocationType");
    const targetLocationType = document.getElementById("comparisonTargetLocationType");
    
    // Source file path handling
    if (sourceLocationType) {
      if (sourceLocationType.value === "unix") {
        const unixPath = document.getElementById("comparisonSourceUnixPath");
        if (unixPath && unixPath.value) {
          data["source_unix_path"] = unixPath.value;
          data["source_hadoop_path"] = "";  // Clear the other path
        }
      } else if (sourceLocationType.value === "hadoop") {
        const hadoopPath = document.getElementById("comparisonSourceHadoopPath");
        if (hadoopPath && hadoopPath.value) {
          data["source_hadoop_path"] = hadoopPath.value;
          data["source_unix_path"] = "";  // Clear the other path
        }
      }
      data["source_location_type"] = sourceLocationType.value;
    }
    
    // Target file path handling
    if (targetLocationType) {
      if (targetLocationType.value === "unix") {
        const unixPath = document.getElementById("comparisonTargetUnixPath");
        if (unixPath && unixPath.value) {
          data["target_unix_path"] = unixPath.value;
          data["target_hadoop_path"] = "";  // Clear the other path
        }
      } else if (targetLocationType.value === "hadoop") {
        const hadoopPath = document.getElementById("comparisonTargetHadoopPath");
        if (hadoopPath && hadoopPath.value) {
          data["target_hadoop_path"] = hadoopPath.value;
          data["target_unix_path"] = "";  // Clear the other path
        }
      }
      data["target_location_type"] = targetLocationType.value;
    }
    
    // Handle file SQL query - default to "N/A" if empty
    const sourceFileSqlQuery = document.getElementById("comparisonSourceFileSqlQuery");
    const targetFileSqlQuery = document.getElementById("comparisonTargetFileSqlQuery");
    
    if (sourceFileSqlQuery) {
      data["source_file_sql_query"] = sourceFileSqlQuery.value.trim() || "N/A";
    }
    if (targetFileSqlQuery) {
      data["target_file_sql_query"] = targetFileSqlQuery.value.trim() || "N/A";
    }
  }

  // Handle Time Zone field - always include it with default "N/A"
  // Check for comparison tab first, then load tab
  let timeZonePrefix = null;
  if (document.getElementById("comparisonTimeZone")) {
    timeZonePrefix = "comparison";
  } else if (document.getElementById("loadTimeZone")) {
    timeZonePrefix = "load";
  }
  if (timeZonePrefix) {
    data["time_zone"] = getTimeZoneValue(timeZonePrefix);
  } else {
    data["time_zone"] = "N/A";
  }

  // Handle Data Load specific fields
  if (tabPrefix === "load") {
    // Target Load Type
    const targetLoadType = document.getElementById("loadTargetLoadType");
    if (targetLoadType) {
      data["target_load_type"] = targetLoadType.value || "";
    }
    
    // Check if target is Hive for Hive-specific fields
    const targetType = document.getElementById("loadTargetType");
    if (targetType && targetType.value === "Hive") {
      const hadoopPath = document.getElementById("loadTargetHadoopPath");
      const partitionId = document.getElementById("loadTargetPartitionId");
      
      if (hadoopPath) {
        data["hadoop_path"] = hadoopPath.value.trim() || "";
      }
      if (partitionId) {
        data["partition_id"] = partitionId.value.trim() || "N/A";
      }
    }
  }

  // Handle File Load specific fields (File-to-Database)
  if (tabPrefix === "fileload") {
    console.log("[FileLoad] Collecting form data for File-to-Database load");
    
    // Source file fields
    const sourceFileType = document.getElementById("fileloadSourceFileType");
    const sourceLocationType = document.getElementById("fileloadSourceLocationType");
    const sourceDelimiter = document.getElementById("fileloadSourceDelimiter");
    const sourceFileSqlQuery = document.getElementById("fileloadSourceFileSqlQuery");
    const sourceKeyColumns = document.getElementById("fileloadSourceKeyColumns");
    
    if (sourceFileType) {
      data["source_file_type"] = sourceFileType.value || "";
    }
    
    if (sourceLocationType) {
      data["source_location_type"] = sourceLocationType.value || "";
      
      if (sourceLocationType.value === "unix") {
        const unixPath = document.getElementById("fileloadSourceUnixPath");
        if (unixPath && unixPath.value) {
          data["source_unix_path"] = unixPath.value;
          data["source_hadoop_path"] = "";
        }
      } else if (sourceLocationType.value === "hadoop") {
        const hadoopPath = document.getElementById("fileloadSourceHadoopPath");
        if (hadoopPath && hadoopPath.value) {
          data["source_hadoop_path"] = hadoopPath.value;
          data["source_unix_path"] = "";
        }
      }
    }
    
    if (sourceDelimiter) {
      data["source_delimiter"] = sourceDelimiter.value || ",";
    }
    
    if (sourceFileSqlQuery) {
      data["source_file_sql_query"] = sourceFileSqlQuery.value.trim() || "N/A";
    }
    
    if (sourceKeyColumns) {
      data["source_key_columns"] = sourceKeyColumns.value || "";
    }
    
    // Target database fields (same as Data Load)
    const targetLoadType = document.getElementById("fileloadTargetLoadType");
    if (targetLoadType) {
      data["target_load_type"] = targetLoadType.value || "";
    }
    
    // Check if target is Hive for Hive-specific fields
    const targetType = document.getElementById("fileloadTargetType");
    if (targetType && targetType.value === "Hive") {
      const hadoopPath = document.getElementById("fileloadTargetHadoopPath");
      const partitionId = document.getElementById("fileloadTargetPartitionId");
      
      if (hadoopPath) {
        data["hadoop_path"] = hadoopPath.value.trim() || "";
      }
      if (partitionId) {
        data["partition_id"] = partitionId.value.trim() || "N/A";
      }
    }
    
    // Handle Time Zone for File Load
    const fileloadTzSelect = document.getElementById("fileloadTimeZone");
    if (fileloadTzSelect) {
      data["time_zone"] = getTimeZoneValue("fileload");
    }
  }

  return data;
}

// ============================================================
// WEBSOCKET CONNECTION (Legacy - kept for backwards compatibility)
// ============================================================
function initializeWebSocket() {
  try {
    socket = io();

    socket.on("connect", function () {
      isConnected = true;
      updateConnectionStatus();
      addLog("🔗 Connected to Wells Fargo Data Management Platform");
    });

    socket.on("disconnect", function () {
      isConnected = false;
      updateConnectionStatus();
      addLog("🔌 Disconnected from server");
    });

    socket.on("log", function (data) {
      if (!isPaused) {
        addLog(data.message);
      }
    });

    socket.on("operation_complete", function (data) {
      console.log("Operation complete:", data);
      if (data.status === "success") {
        jobComplete = true;
        if (data.report_path) {
          reportUrl = data.report_path;
        }
        enableDownloadButton();
        addLog("✅ Comparison complete! Report ready to download.");
      }
    });

    socket.on("operation_error", function (data) {
      console.error("Operation error:", data);
      jobComplete = false;
      reportUrl = "";
      disableDownloadButton();
      addLog(`❌ Comparison failed: ${data.error}`);
    });

    socket.on("error", function (error) {
      console.error("Socket error:", error);
      addLog(`❌ Connection error: ${error}`);
    });
  } catch (error) {
    console.error("WebSocket initialization failed:", error);
    // Don't show error - SSE is the primary method now
  }
}

// Update connection status indicator
function updateConnectionStatus() {
  const statusElement = document.getElementById("connection-status");
  if (!statusElement) return;

  // Show ready state based on SSE support (always available in modern browsers)
  statusElement.innerHTML =
    '<div class="w-2 h-2 bg-green-500 rounded-full"></div>Ready';
  statusElement.className =
    "status-indicator flex items-center gap-2 text-green-600";
}

// Add log message
function addLog(message) {
  // Handle messages that already have timestamps
  let logEntry;
  if (message.startsWith("[")) {
    logEntry = message;
  } else {
    const timestamp = new Date().toLocaleTimeString();
    logEntry = `[${timestamp}] ${message}`;
  }

  logs.push(logEntry);

  // Keep only last 1000 logs
  if (logs.length > 1000) {
    logs = logs.slice(-1000);
  }

  // If paused, buffer the entry instead of appending to DOM
  if (isPaused) {
    pauseBuffer.push(logEntry);
    return;
  }

  appendLogToDOM(logEntry);
}

// Append a single log entry to the DOM
function appendLogToDOM(logEntry) {
  const logDisplay = document.getElementById("log-display");
  if (logDisplay) {
    // Remove placeholder if exists
    const placeholder = logDisplay.querySelector(".log-placeholder");
    if (placeholder) {
      placeholder.remove();
    }

    // Remove "No logs available" message if exists
    const noLogsMsg = logDisplay.querySelector("div.text-gray-500");
    if (noLogsMsg && noLogsMsg.textContent.includes("No logs available")) {
      noLogsMsg.remove();
    }

    const logElement = document.createElement("div");
    logElement.className =
      "log-entry text-sm font-mono mb-1 p-2 rounded hover:bg-gray-800";
    logElement.textContent = logEntry;

    logDisplay.appendChild(logElement);

    logDisplay.scrollTop = logDisplay.scrollHeight;
  }
}

// Initialize log controls
function initializeLogControls() {
  // Clear logs button
  const clearLogsBtn = document.getElementById("clear-logs");
  if (clearLogsBtn) {
    clearLogsBtn.addEventListener("click", clearLogs);
  }

  // Pause logs button
  const pauseLogsBtn = document.getElementById("pause-logs");
  if (pauseLogsBtn) {
    pauseLogsBtn.addEventListener("click", togglePause);
  }

  // Stop execution button (if exists)
  const stopBtn = document.getElementById("stop-execution");
  if (stopBtn) {
    stopBtn.addEventListener("click", stopExecution);
  }
}

// Log management functions
function clearLogs() {
  logs = [];

  // Also clear logs on server if we have a run ID
  if (currentRunId) {
    fetch(`/api/static/clear/${currentRunId}`, { method: "POST" }).catch(
      console.error
    );
  }

  const logDisplay = document.getElementById("log-display");
  if (logDisplay) {
    logDisplay.innerHTML =
      '<div class="log-placeholder text-gray-500">Logs cleared. New logs will appear here...</div>';
  }
}

function togglePause() {
  isPaused = !isPaused;
  const pauseBtn = document.getElementById("pause-logs");
  const pauseIcon = document.getElementById("pause-icon");
  const playIcon = document.getElementById("play-icon");
  const pausedIndicator = document.getElementById("paused-indicator");

  if (pauseBtn) {
    if (isPaused) {
      if (pauseIcon) pauseIcon.classList.add("hidden");
      if (playIcon) playIcon.classList.remove("hidden");
    } else {
      if (pauseIcon) pauseIcon.classList.remove("hidden");
      if (playIcon) playIcon.classList.add("hidden");

      // Flush buffered logs when resuming
      if (pauseBuffer.length > 0) {
        pauseBuffer.forEach(entry => appendLogToDOM(entry));
        pauseBuffer = [];
      }

      // Auto-scroll to bottom when resuming
      const logDisplay = document.getElementById("log-display");
      if (logDisplay) {
        logDisplay.scrollTop = logDisplay.scrollHeight;
      }
    }
  }

  if (pausedIndicator) {
    if (isPaused) {
      pausedIndicator.classList.remove("hidden");
    } else {
      pausedIndicator.classList.add("hidden");
    }
  }
}

// Download Report Button Functions
function enableDownloadButton() {
  const downloadBtn = document.getElementById("download-report-btn");
  const downloadIcon = document.getElementById("download-icon");
  const checkIcon = document.getElementById("check-icon");
  const spinner = document.getElementById("spinner");
  const btnText = document.getElementById("download-btn-text");

  if (downloadBtn) {
    downloadBtn.disabled = false;
    downloadBtn.classList.add("enabled");

    // Hide spinner, show check icon and download icon
    if (spinner) spinner.classList.add("hidden");
    if (checkIcon) checkIcon.classList.remove("hidden");
    if (downloadIcon) downloadIcon.classList.remove("hidden");
    if (btnText) btnText.textContent = "Download Comparison Report";

    // Add click handler
    downloadBtn.onclick = handleDownloadReport;
  }
}

function disableDownloadButton() {
  // Only update download button for Data Comparison tab - DO NOT affect Data Load
  if (currentTab !== "comparison") {
    console.log("[disableDownloadButton] Skipped - not on comparison tab (currentTab=" + currentTab + ")");
    return;
  }
  
  const downloadBtn = document.getElementById("download-report-btn");
  const downloadIcon = document.getElementById("download-icon");
  const checkIcon = document.getElementById("check-icon");
  const spinner = document.getElementById("spinner");
  const btnText = document.getElementById("download-btn-text");

  if (downloadBtn) {
    downloadBtn.disabled = true;
    downloadBtn.classList.remove("enabled");

    // Show spinner, hide icons
    if (spinner) spinner.classList.remove("hidden");
    if (downloadIcon) downloadIcon.classList.add("hidden");
    if (checkIcon) checkIcon.classList.add("hidden");
    if (btnText) btnText.textContent = "Waiting for Comparison...";

    downloadBtn.onclick = null;
  }
}

function resetDownloadButton() {
  disableDownloadButton();
  jobComplete = false;
  reportUrl = "";
}

function handleDownloadReport() {
  if (!reportUrl) {
    addLog("❌ No report available to download");
    return;
  }

  try {
    addLog("📥 Downloading comparison report...");
    
    // Check if this is a direct download URL from the new single_table endpoint
    if (reportUrl.startsWith('/api/static/comparison/download/')) {
      // Direct download - the browser will handle the file save dialog
      window.location.href = reportUrl;
    } else {
      // Legacy download method
      const link = document.createElement("a");
      link.href = `/api/download-report?path=${encodeURIComponent(reportUrl)}`;
      link.download = "comparison_report.csv";
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }
  } catch (error) {
    console.error("Download error:", error);
    addLog(`❌ Download failed: ${error.message}`);
  }
}

// ============================================================
// MISMATCH EXPLORER TAB
// ============================================================

// Mismatch Explorer State
let mismatchJobId = null;
let mismatchRunId = null;
let mismatchCurrentPage = 1;
let mismatchPageSize = 50;
let mismatchTotalRecords = 0;
let mismatchTotalPages = 0;
let mismatchShowOnlyMismatched = true;
let mismatchSelectedPk = null;
let mismatchExecutionStatus = 'IDLE'; // IDLE, RUNNING, SUCCESS, FAILED

/**
 * Generate the Mismatch Explorer form
 */
function generateMismatchExplorerForm() {
  const formContainer = document.getElementById('mismatch-form');
  if (!formContainer) {
    console.error('Mismatch form container not found');
    return;
  }

  // Filter out "File" from database types
  const dbTypesNoFile = databaseTypes.filter(type => type !== "File");

  formContainer.innerHTML = `
    <!-- Source Configuration -->
     <div class="grid lg:grid-cols-2 gap-6 mb-6">
       <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
         <div class="card-header gradient-header section-header-source">
           <h3 class="text-wellsfargo-red flex items-center gap-2">
             <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
             Source Configuration
           </h3>
          <p>Configure your source database connection details</p>
        </div>
        <div class="config-content p-6 space-y-4">
          <div class="form-group">
            <label for="mismatchSourceType" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
            <select id="mismatchSourceType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
              <option value="">Select database type</option>
              ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
            </select>
          </div>
          
          <div class="grid grid-cols-2 gap-4">
            <div class="form-group">
              <label for="mismatchSourceHost" class="block text-sm font-medium mb-2">🌐 Host</label>
              <input type="text" id="mismatchSourceHost" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
            </div>
            
            <div class="form-group">
              <label for="mismatchSourcePort" class="block text-sm font-medium mb-2">🔌 Port</label>
              <input type="text" id="mismatchSourcePort" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
            </div>
          </div>
          
          <div class="form-group">
            <label for="mismatchSourceDatabase" class="block text-sm font-medium mb-2">💾 Database Name</label>
            <input type="text" id="mismatchSourceDatabase" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
          </div>
          
          <div class="grid grid-cols-2 gap-4">
            <div class="form-group">
              <label for="mismatchSourceUsername" class="block text-sm font-medium mb-2">👤 Username</label>
              <input type="text" id="mismatchSourceUsername" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
            </div>
            
            <div class="form-group">
              <label for="mismatchSourcePassword" class="block text-sm font-medium mb-2">🔒 Password</label>
              <input type="password" id="mismatchSourcePassword" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
            </div>
          </div>
          
          <div class="form-group">
            <label for="mismatchSourceTableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
            <input type="text" id="mismatchSourceTableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
          </div>
          
          ${generatePrimaryKeyDropdown("mismatchSource", "red-500")}
          
          <div class="form-group">
            <label for="mismatchSourceSqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
            <textarea id="mismatchSourceSqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
          </div>
        </div>
      </div>
      
       <!-- Target Configuration -->
       <div class="source-target-card border-t-4 border-t-wellsfargo-red shadow-elevated">
         <div class="card-header gradient-header section-header-target">
           <h3 class="text-wellsfargo-red flex items-center gap-2">
             <div class="w-3 h-3 bg-wellsfargo-red rounded-full"></div>
             Target Configuration
           </h3>
          <p>Configure your target database connection details</p>
        </div>
        <div class="config-content p-6 space-y-4">
          <div class="form-group">
            <label for="mismatchTargetType" class="block text-sm font-medium mb-2">🗄️ Database Type</label>
            <select id="mismatchTargetType" class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200">
              <option value="">Select database type</option>
              ${dbTypesNoFile.map(type => `<option value="${type}">${type}</option>`).join("")}
            </select>
          </div>
          
          <div class="grid grid-cols-2 gap-4">
            <div class="form-group">
              <label for="mismatchTargetHost" class="block text-sm font-medium mb-2">🌐 Host</label>
              <input type="text" id="mismatchTargetHost" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="localhost" maxlength="1500">
            </div>
            
            <div class="form-group">
              <label for="mismatchTargetPort" class="block text-sm font-medium mb-2">🔌 Port</label>
              <input type="text" id="mismatchTargetPort" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="5432" maxlength="10">
            </div>
          </div>
          
          <div class="form-group">
            <label for="mismatchTargetDatabase" class="block text-sm font-medium mb-2">💾 Database Name</label>
            <input type="text" id="mismatchTargetDatabase" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter database name" maxlength="255">
          </div>
          
          <div class="grid grid-cols-2 gap-4">
            <div class="form-group">
              <label for="mismatchTargetUsername" class="block text-sm font-medium mb-2">👤 Username</label>
              <input type="text" id="mismatchTargetUsername" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter username" maxlength="100">
            </div>
            
            <div class="form-group">
              <label for="mismatchTargetPassword" class="block text-sm font-medium mb-2">🔒 Password</label>
              <input type="password" id="mismatchTargetPassword" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter password" maxlength="100">
            </div>
          </div>
          
          <div class="form-group">
            <label for="mismatchTargetTableName" class="block text-sm font-medium mb-2">📋 Table Name</label>
            <input type="text" id="mismatchTargetTableName" class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" placeholder="Enter table name" maxlength="255">
          </div>
          
          ${generatePrimaryKeyDropdown("mismatchTarget", "red-500")}
          
          <div class="form-group">
            <label for="mismatchTargetSqlQuery" class="block text-sm font-medium mb-2">📝 SQL Query (Optional)</label>
            <textarea id="mismatchTargetSqlQuery" class="form-textarea w-full p-3 border-2 border-gray-300 rounded-lg focus:border-red-500 focus:ring-2 focus:ring-red-200" rows="3" placeholder="Enter custom SQL query (optional)"></textarea>
          </div>
        </div>
      </div>
    </div>

     <!-- Comparison Rules with Time Zone -->
     <div class="source-target-card border-t-4 border-t-mismatch shadow-elevated mb-6">
       <div class="card-header gradient-header section-header-rules">
         <h3 class="text-mismatch flex items-center gap-2">
           <span class="text-xl">⚙️</span>
           Comparison Rules
         </h3>
        <p>Configure comparison behavior</p>
      </div>
      <div class="config-content p-6">
        <div class="mismatch-rules-grid mb-4">
          <div class="mismatch-rule-item">
            <label>Trim Spaces</label>
            <div class="mismatch-toggle active" id="rule-trim-spaces" onclick="toggleMismatchRule(this)"></div>
          </div>
          <div class="mismatch-rule-item">
            <label>Ignore Case</label>
            <div class="mismatch-toggle" id="rule-ignore-case" onclick="toggleMismatchRule(this)"></div>
          </div>
          <div class="mismatch-rule-item">
            <label>NULL = Empty String</label>
            <div class="mismatch-toggle active" id="rule-null-equals-empty" onclick="toggleMismatchRule(this)"></div>
          </div>
          <div class="mismatch-rule-item">
            <label>Numeric Tolerance</label>
            <input type="number" id="rule-numeric-tolerance" class="form-input" value="0" min="0" step="0.01" style="width: 80px; padding: 0.5rem;">
          </div>
        </div>
        
        <!-- Time Zone field -->
        <div class="form-group mt-4 pt-4 border-t border-gray-200">
          <label for="mismatchTimeZone" class="block text-sm font-medium mb-2">🌍 Time Zone</label>
          <div class="relative">
            <select id="mismatchTimeZone" 
                   class="form-select w-full p-3 border-2 border-gray-300 rounded-lg focus:border-mismatch focus:ring-2 focus:ring-mismatch/20"
                   onchange="handleTimeZoneChange('mismatch')">
              ${commonTimeZones.map(tz => `<option value="${tz}"${tz === 'N/A' ? ' selected' : ''}>${tz}</option>`).join('')}
              <option value="__manual__">Enter Manually</option>
            </select>
          </div>
          <div id="mismatchTimeZoneManualContainer" class="mt-2 hidden">
            <input type="text" 
                   id="mismatchTimeZoneManual" 
                   class="form-input w-full p-3 border-2 border-gray-300 rounded-lg focus:border-mismatch focus:ring-2 focus:ring-mismatch/20" 
                   placeholder="Enter custom time zone (e.g., America/Phoenix)"
                   maxlength="100">
          </div>
          <p class="text-sm text-gray-500 mt-1">Select from the list or choose "Enter Manually" to type a custom time zone. Leave as "N/A" if not applicable.</p>
        </div>
      </div>
    </div>

    <!-- Unix Server Configuration -->
    ${generateUnixServerSection("mismatch")}

    <!-- Action Buttons -->
    <div class="execute-section text-center p-6 border-t border-gray-200">
      <div class="flex justify-center gap-6 flex-wrap">
        <button type="button" id="mismatch-execute-btn" class="execute-button bg-gradient-to-r from-red-600 to-red-700 text-white px-8 py-3 rounded-lg font-semibold hover:from-red-700 hover:to-red-800 transition-all duration-200 shadow-lg" onclick="executeMismatchExplorerJob()">
          <span class="execute-icon mr-2">▶️</span>
          <span id="mismatch-execute-btn-text">Execute</span>
        </button>
        <button type="button" id="mismatch-fetch-btn" style="background: linear-gradient(to right, #059669, #0d9488); color: white; padding: 0.75rem 2rem; border-radius: 0.5rem; font-weight: 600; box-shadow: 0 4px 14px -3px rgba(5,150,105,0.4); border: none; cursor: pointer; transition: all 0.2s; opacity: 1;" onmouseenter="if(!this.disabled){this.style.background='linear-gradient(to right, #047857, #0f766e)'; this.style.boxShadow='0 6px 20px -3px rgba(5,150,105,0.5)'; this.style.transform='translateY(-1px)';}" onmouseleave="this.style.background='linear-gradient(to right, #059669, #0d9488)'; this.style.boxShadow='0 4px 14px -3px rgba(5,150,105,0.4)'; this.style.transform='translateY(0)';" onclick="fetchMismatchResults()" disabled>
          <span class="mr-2">🔍</span>
          <span id="mismatch-fetch-btn-text">Fetch &amp; Compare</span>
        </button>
      </div>
    </div>

    <!-- Results Container -->
    <div id="mismatch-results-container" style="display: none;">
      <div class="mismatch-explorer-container">
        <!-- LEFT: Summary Panel -->
        <div class="mismatch-summary-panel">
          <div class="mismatch-panel-header">
            <h4>📋 Summary</h4>
            <span id="mismatch-record-count" class="text-sm text-muted-foreground"></span>
          </div>
          <div style="padding: 0.5rem 1rem;">
            <input type="text" id="mismatch-summary-search" class="form-control" placeholder="Search by Primary Key" oninput="filterMismatchSummary()" style="width: 100%; padding: 0.4rem 0.75rem; font-size: 0.85rem;">
          </div>
          <div class="mismatch-panel-content" id="mismatch-summary-content">
            <div class="mismatch-empty">
              <div class="mismatch-empty-icon">📊</div>
              <p>Run a comparison to see results</p>
            </div>
          </div>
          <div class="mismatch-pagination" id="mismatch-pagination" style="display: none;">
            <div class="mismatch-pagination-info">
              <span id="mismatch-page-info">Page 1 of 1</span>
            </div>
            <div class="mismatch-pagination-controls">
              <button class="pagination-btn" id="mismatch-prev-btn" onclick="mismatchChangePage(-1)" disabled>‹</button>
              <span id="mismatch-page-numbers"></span>
              <button class="pagination-btn" id="mismatch-next-btn" onclick="mismatchChangePage(1)">›</button>
            </div>
          </div>
        </div>
        
        <!-- RIGHT: Details Panel -->
        <div class="mismatch-details-panel">
          <div class="mismatch-panel-header">
            <h4>🔎 Record Details</h4>
            <div class="mismatch-toggle-container" id="mismatch-toggle-container" style="display: none;">
              <label>Show only mismatches</label>
              <div class="mismatch-toggle active" id="toggle-mismatch-only" onclick="toggleShowOnlyMismatched()"></div>
            </div>
          </div>
          <div style="padding: 0.5rem 1rem;">
            <input type="text" id="mismatch-details-search" class="form-control" placeholder="Search field name" oninput="filterMismatchDetails()" style="width: 100%; padding: 0.4rem 0.75rem; font-size: 0.85rem;">
          </div>
          <div class="mismatch-panel-content" id="mismatch-details-content">
            <div class="mismatch-empty">
              <div class="mismatch-empty-icon">👆</div>
              <p>Click "View Details" on a record to see field-level comparison</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
}

/**
 * Toggle comparison rule
 */
function toggleMismatchRule(element) {
  element.classList.toggle('active');
}

/**
 * Toggle showing only mismatched fields
 */
function toggleShowOnlyMismatched() {
  const toggle = document.getElementById('toggle-mismatch-only');
  toggle.classList.toggle('active');
  mismatchShowOnlyMismatched = toggle.classList.contains('active');
  
  // Re-render details if we have a selected PK
  if (mismatchSelectedPk) {
    loadMismatchDetails(mismatchSelectedPk);
  }
}

/**
 * Collect comparison rules from UI
 */
function collectMismatchRules() {
  return {
    trim_spaces: document.getElementById('rule-trim-spaces').classList.contains('active'),
    ignore_case: document.getElementById('rule-ignore-case').classList.contains('active'),
    null_equals_empty: document.getElementById('rule-null-equals-empty').classList.contains('active'),
    numeric_tolerance: parseFloat(document.getElementById('rule-numeric-tolerance').value) || 0,
    time_zone: getTimeZoneValue('mismatch')
  };
}

/**
 * Collect Mismatch Explorer form data
 */
function collectMismatchFormData() {
  return {
    // Source configuration
    mismatchSourceType: document.getElementById('mismatchSourceType').value,
    mismatchSourceHost: document.getElementById('mismatchSourceHost').value.trim(),
    mismatchSourcePort: document.getElementById('mismatchSourcePort').value.trim(),
    mismatchSourceDatabase: document.getElementById('mismatchSourceDatabase').value.trim(),
    mismatchSourceUsername: document.getElementById('mismatchSourceUsername').value.trim(),
    mismatchSourcePassword: document.getElementById('mismatchSourcePassword').value,
    mismatchSourceTableName: document.getElementById('mismatchSourceTableName').value.trim(),
    mismatchSourcePrimaryKey: resolvePrimaryKeyValue('mismatchSource'),
    mismatchSourceSqlQuery: document.getElementById('mismatchSourceSqlQuery').value.trim(),
    
    // Target configuration
    mismatchTargetType: document.getElementById('mismatchTargetType').value,
    mismatchTargetHost: document.getElementById('mismatchTargetHost').value.trim(),
    mismatchTargetPort: document.getElementById('mismatchTargetPort').value.trim(),
    mismatchTargetDatabase: document.getElementById('mismatchTargetDatabase').value.trim(),
    mismatchTargetUsername: document.getElementById('mismatchTargetUsername').value.trim(),
    mismatchTargetPassword: document.getElementById('mismatchTargetPassword').value,
    mismatchTargetTableName: document.getElementById('mismatchTargetTableName').value.trim(),
    mismatchTargetPrimaryKey: resolvePrimaryKeyValue('mismatchTarget'),
    mismatchTargetSqlQuery: document.getElementById('mismatchTargetSqlQuery').value.trim(),
    
    // Rules
    rules: collectMismatchRules(),
    
    // Unix configuration
    mismatchUnixHost: document.getElementById('mismatchUnixHost').value.trim(),
    mismatchUserEmail: document.getElementById('mismatchUserEmail').value.trim(),
    mismatchBatchId: document.getElementById('mismatchBatchId').value.trim(),
    mismatchUnixUsername: document.getElementById('mismatchUnixUsername').value.trim(),
    mismatchUnixPassword: document.getElementById('mismatchUnixPassword').value
  };
}

/**
 * Execute Mismatch Explorer job via Paramiko
 */
async function executeMismatchExplorerJob() {
  // Collect form data
  const formData = collectMismatchFormData();
  
  // Validation
  if (!formData.mismatchUnixHost) {
    alert('Unix Host is required');
    return;
  }
  if (!formData.mismatchUserEmail) {
    alert('User Email is required');
    return;
  }
  if (!formData.mismatchBatchId) {
    alert('Batch ID is required');
    return;
  }
  if (!formData.mismatchUnixUsername || !formData.mismatchUnixPassword) {
    alert('Unix credentials are required');
    return;
  }
  
  // Update UI to loading state
  const executeBtn = document.getElementById('mismatch-execute-btn');
  const executeBtnText = document.getElementById('mismatch-execute-btn-text');
  const fetchBtn = document.getElementById('mismatch-fetch-btn');
  
  executeBtn.disabled = true;
  executeBtnText.textContent = 'Running...';
  fetchBtn.disabled = true;
  mismatchExecutionStatus = 'RUNNING';
  updateLogStatusRing('RUNNING');
  
  // Clear logs
  clearLogs();
  addLog('🚀 Starting Mismatch Explorer execution...');
  
  try {
    // Add active tab identifier for Unix script
    formData.active_tab = 'mismatch_explorer';
    console.log('Active tab passed to payload: mismatch_explorer');

    const response = await fetch('/api/mismatch-explorer/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(formData)
    });
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || 'Failed to start execution');
    }
    
    mismatchRunId = data.run_id;
    addLog(`📋 Execution started with Run ID: ${mismatchRunId}`);
    
    // Start SSE log streaming
    startMismatchLogStreaming(mismatchRunId);
    
  } catch (error) {
    console.error('Mismatch Explorer execution error:', error);
    addLog(`❌ Execution failed: ${error.message}`);
    mismatchExecutionStatus = 'FAILED';
    updateLogStatusRing('FAILED');
    executeBtn.disabled = false;
    executeBtnText.textContent = 'Execute';
  }
}

/**
 * Start SSE log streaming for Mismatch Explorer
 */
function startMismatchLogStreaming(runId) {
  if (eventSource) {
    eventSource.close();
  }
  
  eventSource = new EventSource(`/api/static/logs/${runId}`);
  
  eventSource.onmessage = function(event) {
    try {
      const data = JSON.parse(event.data);
      
      if (data.message) {
        addLog(data.message);
      }
      
      if (data.status) {
        const executeBtn = document.getElementById('mismatch-execute-btn');
        const executeBtnText = document.getElementById('mismatch-execute-btn-text');
        const fetchBtn = document.getElementById('mismatch-fetch-btn');
        const fetchBtnText = document.getElementById('mismatch-fetch-btn-text');
        
        if (data.status === 'SUCCESS') {
          mismatchExecutionStatus = 'SUCCESS';
          updateLogStatusRing('SUCCESS');
          addLog('✅ Execution completed successfully!');
          addLog('💡 Click "Fetch & Compare" to load results.');
          
          executeBtn.disabled = false;
          executeBtnText.textContent = 'Execute';
          fetchBtn.disabled = false;
          fetchBtnText.textContent = 'Fetch & Compare';
          
          eventSource.close();
        } else if (data.status === 'FAILED') {
          mismatchExecutionStatus = 'FAILED';
          updateLogStatusRing('FAILED');
          addLog('❌ Execution failed.');
          
          executeBtn.disabled = false;
          executeBtnText.textContent = 'Execute';
          fetchBtn.disabled = true;
          
          eventSource.close();
        }
      }
    } catch (e) {
      console.error('Error parsing SSE message:', e);
    }
  };
  
  eventSource.onerror = function(error) {
    console.error('SSE error:', error);
    eventSource.close();
    
    const executeBtn = document.getElementById('mismatch-execute-btn');
    const executeBtnText = document.getElementById('mismatch-execute-btn-text');
    
    executeBtn.disabled = false;
    executeBtnText.textContent = 'Execute';
  };
}

/**
 * Fetch mismatch results from Unix /tmp
 */
async function fetchMismatchResults() {
  if (!mismatchRunId) {
    alert('No execution run ID available. Please execute first.');
    return;
  }
  
  const fetchBtn = document.getElementById('mismatch-fetch-btn');
  const fetchBtnText = document.getElementById('mismatch-fetch-btn-text');
  
  fetchBtn.disabled = true;
  fetchBtnText.textContent = 'Fetching...';
  addLog('📥 Fetching results from Unix...');
  
  try {
    const response = await fetch(`/api/mismatch-explorer/fetch/${mismatchRunId}`);
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || 'Failed to fetch results');
    }
    
    mismatchJobId = data.jobId;
    mismatchCurrentPage = 1;
    mismatchTotalRecords = data.totalRecords;
    mismatchPageSize = 50;
    
    // Show results container
    document.getElementById('mismatch-results-container').style.display = 'block';
    
    // Load summary
    await loadMismatchSummary();
    
    // Reset details panel
    mismatchSelectedPk = null;
    document.getElementById('mismatch-details-content').innerHTML = `
      <div class="mismatch-empty">
        <div class="mismatch-empty-icon">👆</div>
        <p>Click "View Details" on a record to see field-level comparison</p>
      </div>
    `;
    document.getElementById('mismatch-toggle-container').style.display = 'none';
    
    addLog(`✅ Results loaded. Found ${data.totalRecords} record(s).`);
    
  } catch (error) {
    console.error('Fetch results error:', error);
    addLog(`❌ Failed to fetch results: ${error.message}`);
    
    document.getElementById('mismatch-summary-content').innerHTML = `
      <div class="mismatch-error">
        ❌ Error: ${error.message}
      </div>
    `;
  } finally {
    fetchBtn.disabled = false;
    fetchBtnText.textContent = 'Fetch & Compare';
  }
}

/**
 * Load paginated summary
 */
async function loadMismatchSummary() {
  const contentEl = document.getElementById('mismatch-summary-content');
  
  // Show loading
  contentEl.innerHTML = `
    <div class="mismatch-loading">
      <div class="mismatch-loading-spinner"></div>
      <p>Loading summary...</p>
    </div>
  `;
  
  try {
    const response = await fetch(
      `/api/mismatch-explorer/summary?jobId=${mismatchJobId}&page=${mismatchCurrentPage}&pageSize=${mismatchPageSize}`
    );
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || 'Failed to load summary');
    }
    
    mismatchTotalRecords = data.totalRecords;
    mismatchTotalPages = data.totalPages;
    
    // Update record count
    document.getElementById('mismatch-record-count').textContent = `${data.totalRecords} record(s)`;
    
    // Render table
    if (data.rows.length === 0) {
      contentEl.innerHTML = `
        <div class="mismatch-empty">
          <div class="mismatch-empty-icon">📭</div>
          <p>No records found</p>
        </div>
      `;
      document.getElementById('mismatch-pagination').style.display = 'none';
      return;
    }
    
    let tableHtml = `
      <table class="mismatch-summary-table">
        <thead>
          <tr>
            <th>Primary Key</th>
            <th>Mismatches</th>
            <th>Status</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
    `;
    
    // Store rows for client-side filtering
    window._mismatchSummaryRows = data.rows;
    
    data.rows.forEach(row => {
      const statusClass = row.status === 'MATCH' ? 'match' : 'mismatch';
      tableHtml += `
        <tr>
          <td><code>${escapeHtml(row.pkValue)}</code></td>
          <td>${row.mismatchCount}</td>
          <td><span class="status-badge ${statusClass}">${row.status}</span></td>
          <td>
            <button class="btn-view-details" onclick="loadMismatchDetails('${escapeHtml(row.pkValue)}')">
              View Details
            </button>
          </td>
        </tr>
      `;
    });
    
    tableHtml += '</tbody></table>';
    contentEl.innerHTML = tableHtml;
    
    // Re-apply search filter if active
    const searchInput = document.getElementById('mismatch-summary-search');
    if (searchInput && searchInput.value.trim()) {
      filterMismatchSummary();
    }
    
    // Update pagination
    updateMismatchPagination();
    
  } catch (error) {
    console.error('Load summary error:', error);
    contentEl.innerHTML = `
      <div class="mismatch-error">
        ❌ Error loading summary: ${error.message}
      </div>
    `;
  }
}

/**
 * Update pagination controls
 */
function updateMismatchPagination() {
  const paginationEl = document.getElementById('mismatch-pagination');
  
  if (mismatchTotalPages <= 1) {
    paginationEl.style.display = 'none';
    return;
  }
  
  paginationEl.style.display = 'flex';
  
  // Update info
  document.getElementById('mismatch-page-info').textContent = 
    `Page ${mismatchCurrentPage} of ${mismatchTotalPages}`;
  
  // Update buttons
  document.getElementById('mismatch-prev-btn').disabled = mismatchCurrentPage <= 1;
  document.getElementById('mismatch-next-btn').disabled = mismatchCurrentPage >= mismatchTotalPages;
  
  // Generate page numbers (show up to 5 pages)
  let pageNumbersHtml = '';
  const startPage = Math.max(1, mismatchCurrentPage - 2);
  const endPage = Math.min(mismatchTotalPages, startPage + 4);
  
  for (let i = startPage; i <= endPage; i++) {
    const activeClass = i === mismatchCurrentPage ? 'active' : '';
    pageNumbersHtml += `
      <button class="pagination-btn ${activeClass}" onclick="mismatchGoToPage(${i})">${i}</button>
    `;
  }
  
  document.getElementById('mismatch-page-numbers').innerHTML = pageNumbersHtml;
}

/**
 * Change page
 */
function mismatchChangePage(delta) {
  const newPage = mismatchCurrentPage + delta;
  if (newPage >= 1 && newPage <= mismatchTotalPages) {
    mismatchCurrentPage = newPage;
    loadMismatchSummary();
  }
}

/**
 * Go to specific page
 */
function mismatchGoToPage(page) {
  mismatchCurrentPage = page;
  loadMismatchSummary();
}

/**
 * Load field-level details for a specific PK
 */
async function loadMismatchDetails(pkValue) {
  mismatchSelectedPk = pkValue;
  const contentEl = document.getElementById('mismatch-details-content');
  
  // Show loading
  contentEl.innerHTML = `
    <div class="mismatch-loading">
      <div class="mismatch-loading-spinner"></div>
      <p>Loading details...</p>
    </div>
  `;
  
  // Show toggle
  document.getElementById('mismatch-toggle-container').style.display = 'flex';
  
  try {
    const response = await fetch(
      `/api/mismatch-explorer/details?jobId=${mismatchJobId}&pkValue=${encodeURIComponent(pkValue)}`
    );
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(data.error || 'Failed to load details');
    }
    
    renderMismatchDetails(data);
    
  } catch (error) {
    console.error('Load details error:', error);
    contentEl.innerHTML = `
      <div class="mismatch-error">
        ❌ Error loading details: ${error.message}
      </div>
    `;
  }
}

/**
 * Render field-level details
 */
function renderMismatchDetails(data) {
  const contentEl = document.getElementById('mismatch-details-content');
  
  // Filter fields if needed
  let fields = data.fields;
  if (mismatchShowOnlyMismatched) {
    fields = fields.filter(f => f.status === 'MISMATCH');
  }
  
  if (fields.length === 0) {
    contentEl.innerHTML = `
      <div class="mismatch-empty">
        <div class="mismatch-empty-icon">✅</div>
        <p>${mismatchShowOnlyMismatched ? 'No mismatched fields' : 'No fields to display'}</p>
      </div>
    `;
    return;
  }
  
  let html = `
    <div style="margin-bottom: 1rem; display: flex; justify-content: space-between; align-items: center;">
      <h5 style="margin: 0; color: hsl(var(--color-mismatch-dark));">
        🔑 PK = <code>${escapeHtml(data.pkValue)}</code>
      </h5>
      <button class="btn-export-csv" onclick="exportMismatchRecord('${escapeHtml(data.pkValue)}')">
        📥 Export CSV
      </button>
    </div>
    <table class="mismatch-details-table">
      <thead>
        <tr>
          <th>Field Name</th>
          <th>Source Value</th>
          <th>Target Value</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
  `;
  
  // Store fields for client-side filtering
  window._mismatchDetailFields = fields;
  window._mismatchDetailData = data;
  
  fields.forEach(field => {
    const rowClass = field.status === 'MISMATCH' ? 'mismatch-row' : '';
    const statusClass = field.status === 'MATCH' ? 'match' : 'mismatch';
    
    html += `
      <tr class="${rowClass}">
        <td><strong>${escapeHtml(field.fieldName)}</strong></td>
        <td><code>${escapeHtml(String(field.sourceValue))}</code></td>
        <td><code>${escapeHtml(String(field.targetValue))}</code></td>
        <td><span class="status-badge ${statusClass}">${field.status}</span></td>
      </tr>
    `;
  });
  
  html += '</tbody></table>';
  contentEl.innerHTML = html;
  
  // Re-apply search filter if active
  const searchInput = document.getElementById('mismatch-details-search');
  if (searchInput && searchInput.value.trim()) {
    filterMismatchDetails();
  }
}

/**
 * Filter summary rows by primary key search
 */
function filterMismatchSummary() {
  const query = (document.getElementById('mismatch-summary-search').value || '').trim().toLowerCase();
  const table = document.querySelector('#mismatch-summary-content .mismatch-summary-table');
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  rows.forEach((tr, i) => {
    const sourceRow = window._mismatchSummaryRows && window._mismatchSummaryRows[i];
    const pkText = sourceRow ? (sourceRow.pkDisplay || sourceRow.pkValue || '') : (tr.cells[0]?.textContent || '');
    tr.style.display = (!query || pkText.toLowerCase().includes(query)) ? '' : 'none';
  });
}

/**
 * Filter detail rows by field name search
 */
function filterMismatchDetails() {
  const query = (document.getElementById('mismatch-details-search').value || '').trim().toLowerCase();
  const table = document.querySelector('#mismatch-details-content .mismatch-details-table');
  if (!table) return;
  const rows = table.querySelectorAll('tbody tr');
  rows.forEach(tr => {
    const fieldName = (tr.cells[0]?.textContent || '').toLowerCase();
    tr.style.display = (!query || fieldName.includes(query)) ? '' : 'none';
  });
}

/**
 * Export single record comparison as CSV
 */
function exportMismatchRecord(pkValue) {
  if (!mismatchJobId) {
    alert('No active job to export from');
    return;
  }
  
  // Trigger download
  window.location.href = `/api/mismatch-explorer/export?jobId=${mismatchJobId}&pkValue=${encodeURIComponent(pkValue)}`;
  addLog(`📥 Exporting comparison for PK: ${pkValue}`);
}

/**
 * HTML escape helper
 */
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}
