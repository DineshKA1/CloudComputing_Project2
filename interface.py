import sys
import json
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QTextEdit, QPushButton, QLabel, 
                             QSplitter, QDialog, QLineEdit, 
                             QFormLayout, QDialogButtonBox, QMessageBox, 
                             QGraphicsView, QGraphicsScene, QGraphicsItem,
                             QGraphicsEllipseItem, QGraphicsTextItem, QGraphicsLineItem,
                             QGraphicsPathItem, QToolTip, QTableView, QHeaderView)
from PySide6.QtCore import Qt, QSize, QRectF, QPointF, Signal, QAbstractTableModel
from PySide6.QtGui import QIcon, QFont, QColor, QPalette, QPen, QBrush, QPainterPath, QLinearGradient

from preprocessing import Database
from pipesyntaxT import parse_qep_json, sql_to_pipe


class NodeGraphicsItem(QGraphicsPathItem):
    def __init__(self, operation, description, x, y, width, height, color_primary, color_secondary):
        super().__init__()
        
        self.operation = operation
        self.description = description
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        
        path = QPainterPath()
        path.addRoundedRect(QRectF(x - width/2, y - height/2, width, height), 15, 15)
        self.setPath(path)
        
        gradient = QLinearGradient(x - width/2, y - height/2, x + width/2, y + height/2)
        gradient.setColorAt(0, QColor(color_primary))
        gradient.setColorAt(1, QColor(color_secondary))
        
        self.setBrush(QBrush(gradient))
        self.setPen(QPen(QColor("#E0F2F1"), 2))
        
        self.setAcceptHoverEvents(True)
        
    def hoverEnterEvent(self, event):
        self.setPen(QPen(QColor("#FFFFFF"), 3))
        QToolTip.showText(event.screenPos(), self.description)
        super().hoverEnterEvent(event)
        
    def hoverLeaveEvent(self, event):
        self.setPen(QPen(QColor("#E0F2F1"), 2))
        super().hoverLeaveEvent(event)


class ResultTableModel(QAbstractTableModel):
    def __init__(self, data=None):
        super().__init__()
        self._data = data or []
        self._headers = []
        
    def setData(self, data, headers):
        self.beginResetModel()
        self._data = data
        self._headers = headers
        self.endResetModel()
        
    def data(self, index, role):
        if not index.isValid():
            return None
            
        if role == Qt.DisplayRole:
            if 0 <= index.row() < len(self._data) and 0 <= index.column() < len(self._headers):
                return str(self._data[index.row()][index.column()])
        return None
        
    def rowCount(self, parent=None):
        return len(self._data)
        
    def columnCount(self, parent=None):
        return len(self._headers) if self._headers else 0
        
    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal and 0 <= section < len(self._headers):
                return self._headers[section]
            elif orientation == Qt.Vertical and 0 <= section < len(self._data):
                return str(section + 1)
        return None


class QEPTreeView(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setRenderHint(self.renderHints().Antialiasing)
        
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        
        self.setStyleSheet("background: #FAFAFA;")
        
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        
    def wheelEvent(self, event):
        zoom_in_factor = 1.25
        zoom_out_factor = 1 / zoom_in_factor

        if event.angleDelta().y() > 0:
            zoom_factor = zoom_in_factor
        else:
            zoom_factor = zoom_out_factor
            
        self.scale(zoom_factor, zoom_factor)
        
    def visualize_qep(self, qep_root):
        self.scene.clear()
        
        root = self._convert_qep_to_tree(qep_root)
        
        level_spacing = 150
        node_spacing = 200
        
        level_widths = {}
        self._calculate_level_widths(root, 0, level_widths)
        
        positions = {}
        self._position_tree(root, 0, 0, positions, level_widths, node_spacing)
        
        self._draw_tree(root, positions)
        
        self.fitInView(self.scene.sceneRect().adjusted(-50, -50, 50, 50), Qt.KeepAspectRatio)
        
    def _convert_qep_to_tree(self, qep_node):
        if not qep_node:
            return None
            
        operation = qep_node.operation
        if qep_node.properties.get('method'):
            operation = f"{qep_node.properties['method']} {operation}"
            
        cost = None
        startup_cost = None
        total_cost = None
        if qep_node.startup_cost is not None and qep_node.total_cost is not None:
            startup_cost = qep_node.startup_cost
            total_cost = qep_node.total_cost
            cost = f"{startup_cost}..{total_cost}"
            
        props = {}
        for key, value in qep_node.properties.items():
            if value:
                props[key] = value
                
        tree_node = TreeNode(operation, cost, props, qep_node.table, startup_cost, total_cost)
        
        for child in qep_node.children:
            child_node = self._convert_qep_to_tree(child)
            if child_node:
                tree_node.add_child(child_node)
                
        return tree_node
        
    def _calculate_level_widths(self, node, level, level_widths):
        if level not in level_widths:
            level_widths[level] = 0
        
        level_widths[level] += 1
        
        for child in node.children:
            self._calculate_level_widths(child, level + 1, level_widths)
            
    def _position_tree(self, node, level, index, positions, level_widths, node_spacing):
        total_width = level_widths[level] * node_spacing
        start_x = -total_width / 2
        x = start_x + (index + 0.5) * node_spacing
        
        y = level * 150
        
        positions[node] = (x, y)
        
        child_index = 0
        for child in node.children:
            self._position_tree(child, level + 1, child_index, positions, level_widths, node_spacing)
            child_index += 1
    
    def _draw_tree(self, node, positions):
        if node not in positions:
            return
            
        x, y = positions[node]
        
        tooltip = f"Operation: {node.operation}\n"
        if node.table:
            tooltip += f"Table: {node.table}\n"
        if node.total_cost is not None:
            tooltip += f"Total Cost: {node.total_cost}\n"
        if node.startup_cost is not None:
            tooltip += f"Startup Cost: {node.startup_cost}\n"
        for key, value in node.properties.items():
            if value and key not in ['method']:
                tooltip += f"{key.capitalize()}: {value}\n"
        
        primary_color = "#26A69A"
        secondary_color = "#00796B"

        # Assign node colors based on type (Scan, Join, Sort, etc.) for visual distinction
        if "SCAN" in node.operation:
            primary_color = "#42A5F5"
            secondary_color = "#1976D2"
        elif "JOIN" in node.operation:
            primary_color = "#7E57C2"
            secondary_color = "#5E35B1"
        elif "SORT" in node.operation:
            primary_color = "#EF5350"
            secondary_color = "#D32F2F"
        elif "AGGREGATE" in node.operation:
            primary_color = "#FF9800"
            secondary_color = "#F57C00"
            
        node_width = 120
        node_height = 80
        
        node_item = NodeGraphicsItem(
            node.operation, 
            tooltip, 
            x, y, 
            node_width, node_height,
            primary_color, secondary_color
        )
        self.scene.addItem(node_item)
        
        display_text = node.operation
        if node.table:
            display_text = f"{display_text}\n{node.table}"
            
        text_item = QGraphicsTextItem(display_text)
        text_item.setDefaultTextColor(QColor("white"))
        font = QFont("Segoe UI", 9, QFont.Bold)
        text_item.setFont(font)
        
        text_width = text_item.boundingRect().width()
        text_height = text_item.boundingRect().height()
        text_item.setPos(x - text_width/2, y - text_height/2)
        
        self.scene.addItem(text_item)
        
        for child in node.children:
            if child in positions:
                child_x, child_y = positions[child]

                # Draw connection line from this node to its child node
                line = QGraphicsLineItem(x, y + node_height/2, child_x, child_y - node_height/2)
                
                line.setPen(QPen(QColor("#90A4AE"), 2))
                
                self.scene.addItem(line)
                
            self._draw_tree(child, positions)


class TreeNode:
    def __init__(self, operation, cost=None, props=None, table=None, startup_cost=None, total_cost=None):
        self.operation = operation
        self.cost = cost
        self.properties = props or {}
        self.table = table
        self.startup_cost = startup_cost
        self.total_cost = total_cost
        self.children = []
        
    def add_child(self, node):
        self.children.append(node)
        return node


class DatabaseConnectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Database Connection")
        self.resize(400, 200)
        
        layout = QFormLayout(self)
        
        self.dbname = QLineEdit("tpch")
        self.user = QLineEdit("postgres")
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)
        self.host = QLineEdit("localhost")
        self.port = QLineEdit("5432")
        
        layout.addRow("Database Name:", self.dbname)
        layout.addRow("Username:", self.user)
        layout.addRow("Password:", self.password)
        layout.addRow("Host:", self.host)
        layout.addRow("Port:", self.port)
        
        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        
        layout.addRow(buttons)
    
    def get_connection_params(self):
        return {
            "dbname": self.dbname.text(),
            "user": self.user.text(),
            "password": self.password.text(),
            "host": self.host.text(),
            "port": self.port.text()
        }


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("SQL to Pipe-Syntax Converter")
        self.setMinimumSize(1200, 800)
        
        self.db = None
        self.current_qep_root = None
        self.current_result_tab = 0
        
        self.setup_ui()
        self.apply_theme()

    def sanitize_query(self, query: str) -> str:
         return query.strip().rstrip(';')
    
    def setup_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        db_layout = QHBoxLayout()
        db_label = QLabel("Database:")
        db_label.setStyleSheet("color: #455A64; font-weight: bold;")
        
        self.db_status = QLabel("Not connected")
        self.db_status.setStyleSheet("color: #F44336; font-weight: bold;")
        
        connect_btn = QPushButton("Connect")
        connect_btn.clicked.connect(self.connect_database)
        
        db_layout.addWidget(db_label)
        db_layout.addWidget(self.db_status)
        db_layout.addStretch()
        db_layout.addWidget(connect_btn)
        
        main_layout.addLayout(db_layout)
        
        h_splitter = QSplitter(Qt.Horizontal)
        
        left_panel = QSplitter(Qt.Vertical)
        
        query_widget = QWidget()
        query_layout = QVBoxLayout(query_widget)
        
        query_header = QLabel("SQL Query:")
        query_header.setFont(QFont("Segoe UI", 11, QFont.Bold))
        query_header.setStyleSheet("color: #455A64;")
        
        self.query_input = QTextEdit()
        self.query_input.setPlaceholderText("Enter your SQL query here...")
        
        self.execute_btn = QPushButton("Execute")
        self.execute_btn.clicked.connect(self.execute_query)
        
        query_layout.addWidget(query_header)
        query_layout.addWidget(self.query_input)
        query_layout.addWidget(self.execute_btn)
        
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        
        results_header_layout = QHBoxLayout()
        results_header = QLabel("Results:")
        results_header.setFont(QFont("Segoe UI", 11, QFont.Bold))
        results_header.setStyleSheet("color: #455A64;")
        
        # Replace QComboBox with 3 parallel buttons
        self.results_buttons_layout = QHBoxLayout()
        
        self.btn_query_results = QPushButton("Query Results")
        self.btn_pipe_syntax = QPushButton("Pipe-Syntax")
        self.btn_qep = QPushButton("Query Execution Plan (QEP)")
        
        # Set fixed height for buttons
        for btn in [self.btn_query_results, self.btn_pipe_syntax, self.btn_qep]:
            btn.setFixedHeight(32)
        
        # Make buttons toggle-style so only one view is active at a time
        self.btn_query_results.setCheckable(True)
        self.btn_pipe_syntax.setCheckable(True)
        self.btn_qep.setCheckable(True)
        self.btn_query_results.setChecked(True)
        
        # Connect button signals
        self.btn_query_results.clicked.connect(lambda: self.switch_result_tab(0))
        self.btn_pipe_syntax.clicked.connect(lambda: self.switch_result_tab(1))
        self.btn_qep.clicked.connect(lambda: self.switch_result_tab(2))
        
        self.results_buttons_layout.addWidget(self.btn_query_results)
        self.results_buttons_layout.addWidget(self.btn_pipe_syntax)
        self.results_buttons_layout.addWidget(self.btn_qep)
        self.results_buttons_layout.addStretch()
        
        results_header_layout.addWidget(results_header)
        results_header_layout.addLayout(self.results_buttons_layout)
        
        self.result_table_model = ResultTableModel()
        self.result_table = QTableView()
        self.result_table.setModel(self.result_table_model)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.setSelectionBehavior(QTableView.SelectRows)
        self.result_table.setAlternatingRowColors(True)
        
        self.pipe_syntax_output = QTextEdit()
        self.pipe_syntax_output.setReadOnly(True)
        
        self.qep_output = QTextEdit()
        self.qep_output.setReadOnly(True)
        
        results_layout.addLayout(results_header_layout)
        results_layout.addWidget(self.result_table)
        results_layout.addWidget(self.pipe_syntax_output)
        results_layout.addWidget(self.qep_output)
        
        self.pipe_syntax_output.hide()
        self.qep_output.hide()
        
        left_panel.addWidget(query_widget)
        left_panel.addWidget(results_widget)
        left_panel.setSizes([300, 400])
        
        visual_widget = QWidget()
        visual_layout = QVBoxLayout(visual_widget)
        
        visual_header = QLabel("QEP Visualization:")
        visual_header.setFont(QFont("Segoe UI", 11, QFont.Bold))
        visual_header.setStyleSheet("color: #455A64;")
        
        self.qep_visual = QEPTreeView()
        
        visual_layout.addWidget(visual_header)
        visual_layout.addWidget(self.qep_visual)
        
        h_splitter.addWidget(left_panel)
        h_splitter.addWidget(visual_widget)
        
        h_splitter.setSizes([600, 600])
        
        main_layout.addWidget(h_splitter)
        
        self.setCentralWidget(main_widget)
        
        self.statusBar().showMessage("Ready")
        self.statusBar().setStyleSheet("color: #455A64;")
    
    def apply_theme(self):
        app = QApplication.instance()
        
        light_palette = QPalette()
        
        light_palette.setColor(QPalette.Window, QColor("#FAFAFA"))
        light_palette.setColor(QPalette.WindowText, QColor("#455A64"))
        
        light_palette.setColor(QPalette.Base, QColor("#FFFFFF"))
        light_palette.setColor(QPalette.AlternateBase, QColor("#ECEFF1"))
        light_palette.setColor(QPalette.Text, QColor("#37474F"))
        
        light_palette.setColor(QPalette.Button, QColor("#26A69A"))
        light_palette.setColor(QPalette.ButtonText, QColor("white"))
        
        light_palette.setColor(QPalette.Highlight, QColor("#26A69A"))
        light_palette.setColor(QPalette.HighlightedText, QColor("white"))
        
        app.setPalette(light_palette)
        
        # Define styles
        button_style = """
            QPushButton {
                background: #26A69A;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: #2BBBAD;
            }
            QPushButton:pressed, QPushButton:checked {
                background: #00897B;
            }
        """
        
        # Style for tab buttons specifically
        tab_button_style = """
            QPushButton {
                background: #E0F2F1;
                color: #455A64;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: #B2DFDB;
            }
            QPushButton:checked {
                background: #26A69A;
                color: white;
            }
        """
        
        text_edit_style = """
            QTextEdit {
                border: 1px solid #BDBDBD;
                border-radius: 6px;
                padding: 8px;
                background-color: #FFFFFF;
                color: #37474F;
                font-family: 'Consolas', monospace;
                font-size: 10pt;
                selection-background-color: #80CBC4;
            }
        """
        
        table_style = """
            QTableView {
                border: 1px solid #BDBDBD;
                border-radius: 6px;
                background-color: #FFFFFF;
                color: #37474F;
                selection-background-color: #80CBC4;
                selection-color: #37474F;
                gridline-color: #ECEFF1;
            }
            QHeaderView::section {
                background-color: #E0F2F1;
                color: #455A64;
                padding: 4px;
                border: none;
                border-right: 1px solid #BDBDBD;
                border-bottom: 1px solid #BDBDBD;
                font-weight: bold;
            }
            QHeaderView::section:checked {
                background-color: #B2DFDB;
            }
        """
        
        scrollbar_style = """
            QScrollBar:vertical {
                border: none;
                background: #ECEFF1;
                width: 10px;
                margin: 0px;
            }
            QScrollBar::handle:vertical {
                background: #B0BEC5;
                min-height: 20px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical:hover {
                background: #90A4AE;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QScrollBar:horizontal {
                border: none;
                background: #ECEFF1;
                height: 10px;
                margin: 0px;
            }
            QScrollBar::handle:horizontal {
                background: #B0BEC5;
                min-width: 20px;
                border-radius: 5px;
            }
            QScrollBar::handle:horizontal:hover {
                background: #90A4AE;
            }
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
                width: 0px;
            }
        """
        
        self.setStyleSheet(f"""
            QMainWindow {{
                background-color: #FAFAFA;
            }}
            QStatusBar {{
                background-color: #ECEFF1;
                color: #455A64;
            }}
            QSplitter::handle {{
                background-color: #BDBDBD;
            }}
            QSplitter::handle:horizontal {{
                width: 1px;
            }}
            QSplitter::handle:vertical {{
                height: 1px;
            }}
            QLabel {{
                color: #455A64;
            }}
            QToolTip {{
                background-color: #37474F;
                color: #FFFFFF;
                border: 1px solid #62727b;
                border-radius: 4px;
                padding: 5px;
                opacity: 230;
            }}
            {scrollbar_style}
        """)
        gray_button_style = """
            QPushButton {
                background: #CFD8DC;  /* 淡蓝灰 */
                color: #263238;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background: #B0BEC5;
            }
            QPushButton:pressed {
                background: #90A4AE;
            }
        """

        self.execute_btn.setStyleSheet(gray_button_style)

        connect_btn = self.findChild(QPushButton, "")
        execute_btn = self.findChild(QPushButton, "")
        
        for btn in [connect_btn, execute_btn]:
            if btn:
                btn.setStyleSheet(button_style)
        
        # Apply tab button style to the tab buttons
        self.btn_query_results.setStyleSheet(tab_button_style)
        self.btn_pipe_syntax.setStyleSheet(tab_button_style)
        self.btn_qep.setStyleSheet(tab_button_style)
        
        # Apply text edit style
        for textedit in self.findChildren(QTextEdit):
            textedit.setStyleSheet(text_edit_style)
        
        # Apply table style
        self.result_table.setStyleSheet(table_style)
    
    def connect_database(self):
        dialog = DatabaseConnectDialog(self)
        dialog.setStyleSheet("""
            QDialog {
                background-color: #FAFAFA;
                color: #455A64;
            }
            QLabel {
                color: #455A64;
            }
            QLineEdit {
                background-color: #FFFFFF;
                color: #37474F;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                padding: 4px;
            }
            QDialogButtonBox QPushButton {
                background: #26A69A;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QDialogButtonBox QPushButton:hover {
                background: #2BBBAD;
            }
        """)
        
        if dialog.exec():
            params = dialog.get_connection_params()
            
            try:
                self.db = Database(
                    dbname=params["dbname"],
                    user=params["user"],
                    password=params["password"],
                    host=params["host"],
                    port=params["port"]
                )
                self.db.connect()
                
                self.db.execute_query("SELECT 1")
                
                self.db_status.setText(f"Connected to {params['dbname']}")
                self.db_status.setStyleSheet("color: #4CAF50; font-weight: bold;")
                self.statusBar().showMessage("Database connected successfully")
            
            except Exception as e:
                self.db = None
                QMessageBox.critical(self, "Connection Error", f"Failed to connect to database: {str(e)}")
                self.db_status.setText("Not connected")
                self.db_status.setStyleSheet("color: #F44336; font-weight: bold;")
    
    def execute_query(self):
        if not self.db:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
        
        query = self.query_input.toPlainText().strip()

        if not query:
            QMessageBox.warning(self, "Empty Query", "Please enter an SQL query.")
            return
        
        try:
            self.statusBar().showMessage("Executing query...")

            # Get both parsed and original version of the execution plan from the database

            plan_json = self.db.get_plan_json(query)
            original_plan = self.db.get_plan_original(query)
            
            root = parse_qep_json(plan_json)
            self.current_qep_root = root
            
           # Convert query into pipe-syntax string for display
            pipe_syntax = sql_to_pipe(query)


            self.pipe_syntax_output.setText(pipe_syntax)
            self.qep_output.setText(original_plan)
            
            try:
                clean_query = self.sanitize_query(query)
                results = self.db.execute_query(clean_query)
                
                if results and len(results) > 0:
                    cursor = self.db.conn.cursor()
                    # Get only the column names (headers) without fetching actual data again
                    cursor.execute(f"SELECT * FROM ({clean_query}) AS temp LIMIT 0")
                    headers = [desc[0] for desc in cursor.description]
                    cursor.close()
                    
                    processed_results = []
                    for row in results:
                        processed_row = []
                        for item in row:
                            processed_row.append(item)
                        processed_results.append(processed_row)
                    
                    self.result_table_model.setData(processed_results, headers)
                    self.statusBar().showMessage(f"Query executed successfully. {len(results)} rows returned.")
                else:
                    self.result_table_model.setData([], [])
                    self.statusBar().showMessage("Query executed successfully. No results returned.")
            except Exception as e:
                self.result_table_model.setData([], [])
                self.statusBar().showMessage(f"Cannot show query results: {str(e)}")
                print(f"Query result error: {str(e)}")
            
            self.qep_visual.visualize_qep(root)
            self.switch_result_tab(0)
            
        except Exception as e:
            QMessageBox.critical(self, "Query Error", f"Error executing query: {str(e)}")
            self.statusBar().showMessage("Query execution failed")
            print(f"Query execution error: {str(e)}")
    
    def switch_result_tab(self, index):
        # Update tab index
        self.current_result_tab = index
        
        # Update button states
        self.btn_query_results.setChecked(index == 0)
        self.btn_pipe_syntax.setChecked(index == 1)
        self.btn_qep.setChecked(index == 2)
        
        # Hide all widgets
        self.result_table.hide()
        self.pipe_syntax_output.hide()
        self.qep_output.hide()
        
        # Show selected widget
        if index == 0:
            self.result_table.show()
        elif index == 1:
            self.pipe_syntax_output.show()
        else:
            self.qep_output.show()


def execute_conversion():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Segoe UI", 9))
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    execute_conversion()