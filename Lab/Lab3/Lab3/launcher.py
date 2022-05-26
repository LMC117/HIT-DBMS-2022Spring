import sys
from PyQt5.QtWidgets import QApplication, QMainWindow
import pymysql
import Ui_SCT_2

if __name__ == '__main__':
    app = QApplication(sys.argv)
    MainWindow = QMainWindow()
    db = pymysql.connect(host="localhost", user="root", password="Lmc1161181", database="sct")
    # cursor = db.cursor()
    ui = Ui_SCT_2.Ui_MainWindow(db)
    ui.setupUi(MainWindow)
    MainWindow.show()
    exec = app.exec_()
    db.close()
    sys.exit(exec)
