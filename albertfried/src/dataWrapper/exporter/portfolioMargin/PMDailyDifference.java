package dataWrapper.exporter.portfolioMargin;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * @author Zhenghong Dong
 */
public class PMDailyDifference extends PMAbstract {
	private final double	_difference;
	private final double	_requirementToday, _requirementYesterday;

	public PMDailyDifference(String importDate, String symbol, double requirementToday, double requirementYesterday, double difference) {
		super( importDate, symbol );
		_difference = difference;
		_requirementToday = requirementToday;
		_requirementYesterday = requirementYesterday;
	}

	@Override
	public void writeNextForMultipleRecords(Workbook wb, Row row, int index) {
		int i = index;
		final CreationHelper createHelper = wb.getCreationHelper();
		row.createCell( i++ ).setCellValue( createHelper.createRichTextString( getSymbol() ) );
		row.createCell( i++ ).setCellValue( getRequirementToday() );
		row.createCell( i++ ).setCellValue( getRequirementYesterday() );
		row.createCell( i++ ).setCellValue( getDifference() );
	}

	@Override
	public int writeNextForSingleRecord(Workbook wb, Sheet sheet, int index) {
		Row row = sheet.createRow( index++ );
		writeNextForMultipleRecords( wb, row, 0 );
		return index;
	}

	public static int size() {
		return 4;
	}

	public double getDifference() {
		return _difference;
	}

	public double getRequirementToday() {
		return _requirementToday;
	}

	public double getRequirementYesterday() {
		return _requirementYesterday;
	}

	@Override
	public void writeCSV(FileWriter out) throws IOException {}
}
