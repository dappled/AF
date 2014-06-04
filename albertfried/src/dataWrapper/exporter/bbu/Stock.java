package dataWrapper.exporter.bbu;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import utils.ParseDate;
import dataWrapper.RealBasic;

/**
 * @author Zhenghong Dong
 */
public class Stock extends RealBasic {
	private double _price;
	private double _stkRate;
	private double _vol;
	private double _exAmt;
	private String[] _exDate;

	/**
	 * @param symbol
	 */
	public Stock(String symbol) {
		super( symbol );
	}

	@Override
	public void writeNextForMultipleRecords(Workbook wb, Row row, int index) {}

	@Override
	public int writeNextForSingleRecord(Workbook wb, Sheet sheet, int rowNum) {
		return 0;
	}

	@Override
	public void writeCSV(FileWriter out) throws IOException {
		if (bbgSymbol().contains( "TEST" )) return; //ignore test cases
		out.append( "stock" );
		out.append( ',' );
		out.append( bbgSymbol() );
		out.append( ',' );
		out.append( "100" );
		out.append( ',' );
		out.append( "0" );
		out.append( ',' );
		out.append( ParseDate.today );
		out.append( ',' );
		out.append( "98" );
		out.append( '\n' );
	}
	
	public String bbgSymbol() {
		if (getSymbol().isEmpty()) return "ZVZZT US Equity";
		return getSymbol() + " US Equity";
	}
	
	public double getPrice() {
		return _price;
	}
	public void setPrice(double price) {
		this._price = price;
	}
	public double getStkRate() {
		return _stkRate;
	}
	public void setStkRate(double stkRate) {
		this._stkRate = stkRate;
	}
	public double getVol() {
		return _vol;
	}
	public void setVol(double vol) {
		this._vol = vol;
	}
	public double getExAmt() {
		return _exAmt;
	}
	public void setExAmt(double exAmt) {
		this._exAmt = exAmt;
	}
	public String[] getExDate() {
		return _exDate;
	}
	public void setExDate(String[] exDate) {
		this._exDate = exDate;
	}
}
