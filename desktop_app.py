"""
Desktop App Launcher for Data Modelling & Ingestion Console.
Can be launched as a native desktop window via PySide6, or opens directly in your browser.
"""
import sys
import webbrowser

PORT = 8005
URL = f"http://localhost:{PORT}/dashboard"

def launch():
    try:
        from PySide6.QtWidgets import QApplication, QMainWindow
        from PySide6.QtWebEngineWidgets import QWebEngineView
        from PySide6.QtCore import QUrl

        app = QApplication(sys.argv)
        app.setApplicationName("Data Modelling & Ingestion Console")
        window = QMainWindow()
        window.setWindowTitle("Data Modelling & Ingestion Console")
        window.resize(1300, 850)

        web = QWebEngineView()
        web.load(QUrl(URL))
        window.setCentralWidget(web)
        window.show()
        sys.exit(app.exec())
    except ImportError:
        print(f"Launching Ingestion Console in your browser: {URL}")
        webbrowser.open(URL)

if __name__ == "__main__":
    launch()
