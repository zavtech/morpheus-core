package com.zavtech.morpheus.source;

public class ExcelSourceOptions<R> extends CsvSourceOptions<R> {

    String sheetName;

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }
}
