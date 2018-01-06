package com.zavtech.morpheus.source;


public class ExcelSourceOptions<R> extends CsvSourceOptions<R> {

    String sheetName;

    ExcelType excelType = ExcelType.XLSX;

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    public ExcelType getExcelType() {
        return excelType;
    }

    public void setExcelType(ExcelType excelType){
        this.excelType = excelType;
    }

    @Override
    public void setResource(String resource) {
        super.setResource(resource);
        setExcelType( resource.endsWith(ExcelType.XLS.type)? ExcelType.XLS: ExcelType.XLSX);
    }

    public enum ExcelType{
        XLS("xls"), XLSX("xlsx");

        String type;
        ExcelType(String type){
            this.type = type;
        }

        public static ExcelType getTypeEnum(String extension){
            if ( XLS.type.equals(extension)){
                return XLS;
            }else if(XLSX.type.equals(extension)){
                return XLSX;
            }
            throw new IllegalArgumentException("Unknown type " + extension);
        }
    }
}
