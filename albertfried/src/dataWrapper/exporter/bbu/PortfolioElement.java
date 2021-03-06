package dataWrapper.exporter.bbu;

import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import utils.ParseDate;
import utils.StringUtils;
import dataWrapper.RealBasic;

/**
 * 
 * @author Zhenghong Dong
 */
public class PortfolioElement extends RealBasic {
	private final Double	_tdQty, _strike, _baseMarketPrice;
	private final String	_putCall, _OCCCode, _account, _type;
	private final String	_maturity;

	public PortfolioElement(String symbol, Double tdQty, String maturity, Double strike, String putCall, Double baseMarketPrice, String OCCCode, String account,
			String type) {
		super( symbol );
		_tdQty = tdQty;
		_strike = strike;
		_baseMarketPrice = baseMarketPrice;
		_putCall = putCall;
		_OCCCode = OCCCode;
		_account = account;
		_maturity = maturity;
		_type = type;
	}

	@Override
	public void writeNextForMultipleRecords(Workbook wb, Row row, int index) {}

	@Override
	public int writeNextForSingleRecord(Workbook wb, Sheet sheet, int rowNum) {
		return 0;
	}

	@Override
	public void writeCSV(FileWriter out) throws IOException {
		if (bbgSymbol().contains( "TEST" )) return; // ignore test cases
		out.append( _type );
		out.append( ',' );
		out.append( bbgSymbol() );
		out.append( ',' );
		out.append( String.valueOf( getTDQty() ) );
		out.append( ',' );
		out.append( round() );
		out.append( ',' );
		out.append( ParseDate.today );
		out.append( ',' );
		out.append( "98" );
		out.append( '\n' );
	}

	public String bbgSymbol() {
		if (getSymbol().isEmpty()) return "ZVZZT US Equity"; // this happens when the symbol is null
		if (getSymbol().endsWith( " Equity" )) return getSymbol(); // this happens when we already made change using clearing.dbo.bbuSymbolCorrection
		else {
			if (getMaturity() == null) {
				return getSymbol() + " US Equity";
			} else {
				return getSymbol() + " US " + getMaturity() + " " + getPutCall()
						+ StringUtils.numberToStringWithoutZeros( getStrike() ) + " Equity";
			}
		}
	}

	public String round() {
		DecimalFormat df = new DecimalFormat( "#.##" ); // we only care abt minimum 1 penny
		df.setRoundingMode( RoundingMode.UP );
		return df.format( getBaseMarketPrice() );
	}

	public Double getTDQty() {
		return _tdQty;
	}

	public Double getStrike() {
		return _strike;
	}

	public Double getBaseMarketPrice() {
		return _baseMarketPrice;
	}

	public String getPutCall() {
		return _putCall;
	}

	public String getOCCCode() {
		return _OCCCode;
	}

	public String getAccount() {
		return _account;
	}

	public String getMaturity() {
		return _maturity;
	}
}
