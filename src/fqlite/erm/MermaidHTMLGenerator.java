package fqlite.erm;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


/**
 * This class creates a html-page with a graphical representation of the
 * database schema.
 * The visualization will be done with mermaid.js framework.
 *
 * @author pawlaszc
 */
public class MermaidHTMLGenerator {

    // Configuration for library paths
    private static String mermaidLibraryPath = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js";
    private static String panzoomLibraryPath = "https://unpkg.com/panzoom@9.4.3/dist/panzoom.min.js";

    /**
     * Set the path to the Mermaid library (local or remote)
     *
     * @param path Path to mermaid.min.js (e.g., "./js/mermaid.min.js" or CDN URL)
     */
    public static void setMermaidLibraryPath(String path) {
        mermaidLibraryPath = path;
    }

    /**
     * Set the path to the Panzoom library (local or remote)
     *
     * @param path Path to panzoom.min.js (e.g., "./js/panzoom.min.js" or CDN URL)
     */
    public static void setPanzoomLibraryPath(String path) {
        panzoomLibraryPath = path;
    }

    /**
     * Reset library paths to default CDN URLs
     */
    public static void resetLibraryPaths() {
        mermaidLibraryPath = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js";
        panzoomLibraryPath = "https://unpkg.com/panzoom@9.4.3/dist/panzoom.min.js";
    }


    // this is the actual html-template that is used for schema information injection
    private static final String HTML_TEMPLATE = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Database ERM View</title>
                <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
                <script src="https://unpkg.com/panzoom@9.4.0/dist/panzoom.min.js"></script>
            
         
                <style>
                    body {
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }
                    .container {
                        max-width: 1400px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 30px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    h1 {
                        color: #333;
                        margin-bottom: 20px;
                    }
                    .info {
                        background-color: #e3f2fd;
                        padding: 15px;
                        border-radius: 4px;
                        margin-bottom: 20px;
                        color: #1976d2;
                    }
                    .diagram-wrapper {
                        background-color: #fafafa;
                        border-radius: 4px;
                        overflow: hidden;
                        position: relative;
                        border: 2px solid #e0e0e0;
                    }
                    .diagram-container {
                        padding: 20px;
                        cursor: grab;
                        min-height: 400px;
                    }
                    .diagram-container:active {
                        cursor: grabbing;
                    }
                    .zoom-controls {
                        position: absolute;
                        top: 10px;
                        right: 10px;
                        background-color: white;
                        border-radius: 4px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                        padding: 8px;
                        display: flex;
                        flex-direction: column;
                        gap: 5px;
                        z-index: 10;
                    }
                    .zoom-btn {
                        background-color: #4CAF50;
                        color: white;
                        border: none;
                        border-radius: 4px;
                        width: 36px;
                        height: 36px;
                        cursor: pointer;
                        font-size: 18px;
                        font-weight: bold;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                    }
                    .zoom-btn:hover {
                        background-color: #45a049;
                    }
                    .zoom-level {
                        text-align: center;
                        font-size: 12px;
                        color: #666;
                        padding: 5px 0;
                    }
                    .controls {
                        margin-bottom: 20px;
                        display: flex;
                        gap: 10px;
                        flex-wrap: wrap;
                    }
                    button {
                        background-color: #4CAF50;
                        color: white;
                        padding: 10px 20px;
                        border: none;
                        border-radius: 4px;
                        cursor: pointer;
                    }
                    button:hover {
                        background-color: #45a049;
                    }
                    button.secondary {
                        background-color: #2196F3;
                    }
                    button.secondary:hover {
                        background-color: #0b7dda;
                    }
                    textarea {
                        width: 100%;
                        height: 300px;
                        font-family: monospace;
                        padding: 10px;
                        border: 1px solid #ddd;
                        border-radius: 4px;
                        margin-top: 10px;
                    }
                    .footer {
                        margin-top: 30px;
                        padding-top: 20px;
                        border-top: 1px solid #e0e0e0;
                        color: #666;
                        font-size: 12px;
                    }
                    .help-text {
                        background-color: #fff9c4;
                        padding: 10px;
                        border-radius: 4px;
                        margin-bottom: 15px;
                        font-size: 14px;
                        color: #666;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>FQLite Schema-Analyzer</h1>
                    
                    <div class="info">
                        ℹ️ Generated on: {{GENERATION_DATE}}<br>
                        Source: Automatically generated from SQL schema
                    </div>
                    
                    <div class="help-text">
                        💡 <strong>Zoom & Navigation:</strong> Mouse wheel to zoom | Drag with mouse to pan | Double-click to reset
                    </div>
                    
                    <div class="controls">
                        <button class="secondary" onclick="resetZoom()">Reset Zoom</button>
                        <button class="secondary" onclick="fitToScreen()">Fit to Screen</button>
                        <button onclick="exportSVG()">Export as SVG</button>
                    </div>
                    
                    <div class="diagram-wrapper">
                        <div class="zoom-controls">
                            <button class="zoom-btn" onclick="zoomIn()" title="Zoom In">+</button>
                            <div class="zoom-level" id="zoom-level">100%</div>
                            <button class="zoom-btn" onclick="zoomOut()" title="Zoom Out">−</button>
                            <button class="zoom-btn" onclick="resetZoom()" title="Reset">⟲</button>
                        </div>
                        <div class="diagram-container" id="diagram-container">
                            <div class="mermaid" id="mermaid-diagram">
                                {{MERMAID_CODE}}
                            </div>
                        </div>
                    </div>
                </div>
            
              <script>
                    let panzoomInstance;
                    let currentZoom = 1;
            
                    // Initialize Mermaid
                    mermaid.initialize({
                        startOnLoad: true,
                        theme: 'default',
                        er: {
                            useMaxWidth: false
                        }
                    });
            
                    // Initialize Pan & Zoom after Mermaid renders
                    mermaid.run().then(() => {
                        setTimeout(initializePanZoom, 100);
                    });
            
                    function initializePanZoom() {
                        const container = document.getElementById('mermaid-diagram');
            
                        if (panzoomInstance) {
                            panzoomInstance.dispose();
                        }
            
                        panzoomInstance = panzoom(container, {
                            maxZoom: 5,
                            minZoom: 0.1,
                            bounds: false,
                            zoomSpeed: 0.065,
                            beforeWheel: function(e) {
                                return true;
                            },
                            onTouch: function(e) {
                                return true;
                            }
                        });
            
                        // Update zoom level display
                        panzoomInstance.on('zoom', function(e) {
                            currentZoom = e.getTransform().scale;
                            updateZoomLevel();
                        });
            
                        // Double-click to reset
                        container.addEventListener('dblclick', function(e) {
                            e.preventDefault();
                            resetZoom();
                        });
            
                        updateZoomLevel();
                    }
            
                    function updateZoomLevel() {
                        const zoomPercent = Math.round(currentZoom * 100);
                        document.getElementById('zoom-level').textContent = zoomPercent + '%';
                    }
            
                    function zoomIn() {
                        if (panzoomInstance) {
                            const container = document.getElementById('diagram-container');
                            const rect = container.getBoundingClientRect();
                            const centerX = rect.width / 2;
                            const centerY = rect.height / 2;
                            panzoomInstance.smoothZoom(centerX, centerY, 1.3);
                        }
                    }
            
                    function zoomOut() {
                        if (panzoomInstance) {
                            const container = document.getElementById('diagram-container');
                            const rect = container.getBoundingClientRect();
                            const centerX = rect.width / 2;
                            const centerY = rect.height / 2;
                            panzoomInstance.smoothZoom(centerX, centerY, 0.77);
                        }
                    }
            
                    function resetZoom() {
                        if (panzoomInstance) {
                            panzoomInstance.moveTo(0, 0);
                            panzoomInstance.zoomTo(0, 0, 1);
                            currentZoom = 1;
                            updateZoomLevel();
                        }
                    }
            
                    function fitToScreen() {
                        if (panzoomInstance) {
                            const container = document.getElementById('diagram-container');
                            const mermaidDiv = document.getElementById('mermaid-diagram');
                            const svg = mermaidDiv.querySelector('svg');
            
                            if (svg) {
                                const containerRect = container.getBoundingClientRect();
                                const svgRect = svg.getBoundingClientRect();
            
                                const scaleX = (containerRect.width - 40) / svgRect.width;
                                const scaleY = (containerRect.height - 40) / svgRect.height;
                                const scale = Math.min(scaleX, scaleY, 1);
            
                                panzoomInstance.moveTo(0, 0);
                                panzoomInstance.zoomTo(0, 0, scale);
                                currentZoom = scale;
                                updateZoomLevel();
                            }
                        }
                    }
            
                    function exportSVG() {
                        const svg = document.querySelector('#mermaid-diagram svg');
                        if (svg) {
                            const serializer = new XMLSerializer();
                            const svgString = serializer.serializeToString(svg);
                            const blob = new Blob([svgString], { type: 'image/svg+xml' });
                            const url = URL.createObjectURL(blob);
                            const link = document.createElement('a');
                            link.href = url;
                            link.download = 'erm-diagram.svg';
                            link.click();
                            URL.revokeObjectURL(url);
                        }
                    }
                </script>
            </body>
            </html>
            """;

    /**
     * Generates an HTML file with embedded Mermaid diagram
     *
     * @param mermaidCode The Mermaid code to embed in the HTML file
     * @param outputPath The path where the HTML file should be saved
     * @throws IOException On file writing errors
     */
    public static void generateHTMLFile(String mermaidCode, String outputPath) throws IOException {
        // Current date and time for metadata
        String generationDate = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"));

        // Get template and fill with library paths, Mermaid code and date
        String htmlContent = HTML_TEMPLATE
                .replace("{{MERMAID_PATH}}", mermaidLibraryPath)
                .replace("{{PANZOOM_PATH}}", panzoomLibraryPath)
                .replace("{{MERMAID_CODE}}", mermaidCode)
                .replace("{{GENERATION_DATE}}", generationDate);

        // Write file
        Files.writeString(Paths.get(outputPath), htmlContent, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        System.out.println("✅ HTML file successfully created: " + outputPath);
    }





}
