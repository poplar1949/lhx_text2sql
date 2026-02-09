import json
import sys
from typing import Any, Dict

from PyQt5 import QtCore, QtWidgets

from app.core.engine import Text2SQLEngine


class LocalWorker(QtCore.QThread):
    result = QtCore.pyqtSignal(dict)
    error = QtCore.pyqtSignal(str)

    def __init__(self, engine: Text2SQLEngine, payload: Dict[str, Any]) -> None:
        super().__init__()
        self.engine = engine
        self.payload = payload

    def run(self) -> None:
        try:
            question = self.payload.get("question", "")
            user_context = self.payload.get("user_context", {})
            time_range = self.payload.get("time_range")
            data = self.engine.run_query(question, user_context, time_range)
            self.result.emit(data)
        except Exception as exc:
            self.error.emit(str(exc))


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Text2SQL Planner UI")
        self.resize(1100, 700)
        self.engine = Text2SQLEngine()
        self._worker: LocalWorker | None = None
        self._build_ui()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        root = QtWidgets.QVBoxLayout(central)

        form = QtWidgets.QFormLayout()
        self.question_edit = QtWidgets.QLineEdit()
        self.role_edit = QtWidgets.QLineEdit("analyst")
        self.tenant_edit = QtWidgets.QLineEdit("demo")
        self.start_edit = QtWidgets.QLineEdit("2024-01-01")
        self.end_edit = QtWidgets.QLineEdit("2024-01-31")
        form.addRow("Question", self.question_edit)
        form.addRow("Role", self.role_edit)
        form.addRow("Tenant", self.tenant_edit)
        form.addRow("Start Date", self.start_edit)
        form.addRow("End Date", self.end_edit)
        root.addLayout(form)

        btn_row = QtWidgets.QHBoxLayout()
        self.send_btn = QtWidgets.QPushButton("Run Query")
        self.test_btn = QtWidgets.QPushButton("Test Connections")
        self.clear_btn = QtWidgets.QPushButton("Clear")
        btn_row.addWidget(self.send_btn)
        btn_row.addWidget(self.test_btn)
        btn_row.addWidget(self.clear_btn)
        btn_row.addStretch(1)
        root.addLayout(btn_row)

        self.status_label = QtWidgets.QLabel("Ready.")
        root.addWidget(self.status_label)

        self.tabs = QtWidgets.QTabWidget()
        self.answer_text = self._make_readonly_text()
        self.sql_text = self._make_readonly_text()
        self.plan_text = self._make_readonly_text()
        self.debug_text = self._make_readonly_text()
        self.raw_text = self._make_readonly_text()
        self.preview_table = QtWidgets.QTableWidget()
        self.preview_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.preview_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

        self.tabs.addTab(self.answer_text, "Answer")
        self.tabs.addTab(self.sql_text, "SQL")
        self.tabs.addTab(self.plan_text, "Plan DSL")
        self.tabs.addTab(self.preview_table, "Data Preview")
        self.tabs.addTab(self.debug_text, "Debug")
        self.tabs.addTab(self.raw_text, "Raw JSON")
        root.addWidget(self.tabs, 1)

        self.setCentralWidget(central)

        self.send_btn.clicked.connect(self.send_query)
        self.test_btn.clicked.connect(self.test_connections)
        self.clear_btn.clicked.connect(self.clear_output)

    @staticmethod
    def _make_readonly_text() -> QtWidgets.QPlainTextEdit:
        widget = QtWidgets.QPlainTextEdit()
        widget.setReadOnly(True)
        return widget

    def send_query(self) -> None:
        if self._worker and self._worker.isRunning():
            self._set_status("Query already running.")
            return
        payload = self._build_payload()
        self._worker = LocalWorker(self.engine, payload)
        self._worker.result.connect(self._handle_result)
        self._worker.error.connect(self._handle_error)
        self._worker.start()
        self._set_status("Query running...")

    def clear_output(self) -> None:
        self.answer_text.clear()
        self.sql_text.clear()
        self.plan_text.clear()
        self.debug_text.clear()
        self.raw_text.clear()
        self.preview_table.clear()
        self.preview_table.setRowCount(0)
        self.preview_table.setColumnCount(0)

    def _handle_result(self, data: Dict[str, Any]) -> None:
        self._set_status("Response received.")
        self.answer_text.setPlainText(data.get("answer_text", ""))
        self.sql_text.setPlainText(data.get("sql", ""))
        plan = data.get("plan_dsl", {})
        debug = data.get("debug", {})
        self.plan_text.setPlainText(json.dumps(plan, ensure_ascii=False, indent=2))
        self.debug_text.setPlainText(json.dumps(debug, ensure_ascii=False, indent=2))
        self.raw_text.setPlainText(
            json.dumps(data, ensure_ascii=False, indent=2, default=str)
        )
        self._fill_preview_table(data.get("data_preview", {}))

    def _handle_error(self, message: str) -> None:
        self._set_status(f"Error: {message}")
        QtWidgets.QMessageBox.warning(self, "Query Error", message)

    def _fill_preview_table(self, preview: Dict[str, Any]) -> None:
        columns = preview.get("columns", []) or []
        rows = preview.get("rows", []) or []
        self.preview_table.setColumnCount(len(columns))
        self.preview_table.setHorizontalHeaderLabels([str(c) for c in columns])
        self.preview_table.setRowCount(len(rows))
        for r_idx, row in enumerate(rows):
            for c_idx, value in enumerate(row):
                item = QtWidgets.QTableWidgetItem(str(value))
                self.preview_table.setItem(r_idx, c_idx, item)
        self.preview_table.resizeColumnsToContents()

    def _build_payload(self) -> Dict[str, Any]:
        question = self.question_edit.text().strip()
        role = self.role_edit.text().strip()
        tenant = self.tenant_edit.text().strip()
        start = self.start_edit.text().strip()
        end = self.end_edit.text().strip()
        payload = {
            "question": question,
            "user_context": {"role": role, "tenant": tenant},
        }
        if start and end:
            payload["time_range"] = {"start": start, "end": end}
        return payload

    def _set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def test_connections(self) -> None:
        self._set_status("Testing connections...")
        results = self.engine.test_connections()
        message = f"LLM: {results.get('llm')}\nMySQL: {results.get('mysql')}"
        self._set_status("Connection test done.")
        QtWidgets.QMessageBox.information(self, "Connection Test", message)

def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
