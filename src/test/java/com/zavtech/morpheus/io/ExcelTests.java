package com.zavtech.morpheus.io;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.util.Predicates;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.zavtech.morpheus.source.ExcelSourceOptions.ExcelType.getTypeEnum;
import static org.testng.Assert.*;

/**
 * A unit test of the DataFrame Excel reader
 *
 * @author  Dwight Gunning
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ExcelTests {

    @DataProvider(name="excelType")
    public Object[][] excelTypes() {
        return new Object[][] {
                {"xlsx"},
                {"xls"}
        };
    }

    @Test(dataProvider = "excelType")
    public void readFromInputStream(String excelType) throws IOException{
        try(InputStream excelInputStream = getClass().getResourceAsStream("/xls/cars93." + excelType)){
            DataFrame<Integer,String> frame = DataFrame.read().excel( options ->{
                options.setInputStream(excelInputStream);
                options.setExcelType(getTypeEnum(excelType));
            });
            assertNotNull(frame);
            assertEquals(frame.rowCount(), 93);
            assertEquals(frame.cols().count(), 28);
        }
    }

    @Test(dataProvider = "excelType")
    public void readFromFile(String excelType){
        DataFrame<Integer,String> frame = DataFrame.read().excel( options ->{
            options.setResource("/xls/cars93." + excelType);
            // No need to set the type - it's autodetected from the extension
        });
        assertNotNull(frame);
        assertEquals(frame.rowCount(), 93);
        assertEquals(frame.cols().count(), 28);
    }



    @Test(dataProvider = "excelType")
    public void readFromStringResource(String excelType){
        DataFrame<Integer,String> frame = DataFrame.read().excel("/xls/cars93." + excelType);
        assertNotNull(frame);
        assertEquals(frame.rowCount(), 93);
        assertEquals(frame.cols().count(), 28);

        assertTrue(frame.cols().keys().allMatch(Predicates.in(
                "", "Manufacturer","Model","Type","Min.Price","Price","Max.Price","MPG.city","MPG.highway","AirBags","DriveTrain",
                "Cylinders","EngineSize","Horsepower","RPM","Rev.per.mile",	"Man.trans.avail",	"Fuel.tank.capacity",
                "Passengers","Length","Wheelbase","Width","Turn.circle","Rear.seat.room","Luggage.room","Weight","Origin","Make"
        )));
    }

    @Test
    public void readXlsKeys() {
        DataFrame<Integer, String> carsCsv = DataFrame.read().csv("/csv/cars93.csv");
        DataFrame<Integer, String> carsXls = DataFrame.read().excel("/xls/cars93.xls");
        assertEquals( carsCsv.cols().count(),28);
        assertEquals( carsXls.cols().count(),28);
    }

    @Test()
    public void testBasicRead() throws Exception {
        final DataFrame<Integer, String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.getFormats().setParser("Volume", Long.class);
        });
        assertTrue(frame.rows().count() > 0, "There is at least one row");
        assertTrue(frame.cols().count() == 7, "There are 7 columns");
    }

    @Test
    public void excelReadOverBatchLimit(){
        final DataFrame<Integer,String> frame = DataFrame.read().excel( options->{
            options.setResource("/xls/aapl.xlsx");
        });
        assertNotNull(frame);
        frame.row(0).getValue("Open");
        assertEquals(frame.rowCount(), 8503);
        assertEquals(frame.cols().count() ,7);
        assertTrue(frame.cols().keys().allMatch(Predicates.in("Date","Open", "High", "Low", "Close", "Volume", "Adj Close")));

        assertEquals(frame.cols().type("Open"), Double.class);
        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Integer.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);

        assertTrue(frame.rows().firstKey().equals(Optional.of(0)));
        assertTrue(frame.rows().lastKey().equals(Optional.of(8502)));

        assertEquals(frame.data().getDouble(0, "Open"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(0, "High"), 28.87472, 0.00001);
        assertEquals(frame.data().getDouble(0, "Low"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(0, "Close"), 28.74984, 0.00001);
        assertEquals(frame.data().getInt(0, "Volume"), 117258400L);
        assertEquals(frame.data().getDouble(0, "Adj Close"), 0.44203, 0.00001);
    }

    @Test
    public void excelReadWithDatatypes(){
        final DataFrame<Integer,String> frame = DataFrame.read().excel( options->{
            options.setResource("/xls/aapl.xlsx");
            options.setColumnType("Volume", Double.class);
        });

        assertEquals(frame.cols().type("Volume"), Double.class);
        assertEquals(frame.data().getDouble(0, "Volume"), 117258400L, 0.0);
    }

    @Test
    public void excelReadParallel(){
        final DataFrame<Integer,String> frame = DataFrame.read().excel( options->{
            options.setResource("/xls/aapl.xlsx");
            options.setParallel(true);
            options.setColumnType("Volume", Double.class);
        });
        assertEquals(frame.rowCount(), 8503);
        assertEquals(frame.cols().count() ,7);
    }

    @Test(dataProvider = "excelType")
    public void readFromNamedSheet(String excelType){
        final String fileName = "/xls/ApplesAndCars." + excelType;
        DataFrame<Integer,String> frame = DataFrame.read().excel(fileName);
        assertEquals(frame.cols().count() ,7);

        frame = DataFrame.read().excel(options ->{
            options.setResource(fileName);
            options.setSheetName("Apples");
        });
        assertEquals(frame.cols().count() ,7);

        frame = DataFrame.read().excel(options ->{
            options.setResource(fileName);
            options.setSheetName("Cars");
        });
        assertEquals(frame.cols().count() ,7);
    }
}
