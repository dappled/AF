package exporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import utils.ParseDate;
import dataWrapper.exporter.bbu.PortfolioElement;
import dataWrapper.exporter.bbu.Stock;

/**
 * @author Zhenghong Dong
 */
public class BBU extends ExporterBase {
	private HashMap<String, String>	_nameCorrection;
	private final String			_bbuUserName		= "u25210279";
	private final String			_bbuPassword		= "ly6P%XS+";
	private final String			_vineyardUserName	= "Vinyard";
	private final String			_vineyardPassword	= "v1n3yrd";

	/**
	 * @param dbServer
	 * @param catalog
	 */
	public BBU(String dbServer, String catalog) {
		super(dbServer, catalog);
		getNameCorrection();
	}

	/** get Bbu Name corrections from database */
	private void getNameCorrection() {
		_nameCorrection = new HashMap<>();

		final String query = "SELECT [Symbol], [DealId] from [clearing].[dbo].[BbuSymbolCorrection]";

		try (Statement stmt = _conn.createStatement()) {

			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int j = 1;
				_nameCorrection.put(rs.getString(j++).trim(), rs.getString(j++).trim());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void report(String inFile, String outFile, String date, String ftpAddress) throws Exception {
		/* seperate gsec position files and p3 position files */
		String[] tmp = outFile.split(";");
		if (tmp.length != 3) { throw new Exception("outFile should be in the format 'stock outfile; portfolioElements outfile, p3Positions outFile'"); }
		String vinyardOut = tmp[0] + ';' + tmp[1];
		String P3Out = tmp[2];

		/* separate ftpAddress */
		String vinyardFtp = null, bbuFtp = null;
		if (ftpAddress != null) {
			for (String address : ftpAddress.split(";")) {
				String[] detail = address.split(":");
				if (detail[0].equals("vineyard")) {
					vinyardFtp = detail[1];
				} else if (detail[0].equals("bbu")) {
					bbuFtp = detail[1];
				} else throw new Exception("ftp type should be either vinyard or bbu");
			}
		}

		/* GSEC positions */
		List<Stock> stocks = getStock();
		List<PortfolioElement> portfolioElements = getPortfolioElement();
		exportCSVGSEC(stocks, portfolioElements, vinyardOut);
		if (ftpAddress != null) {
			this.uploadFtp(vinyardOut, vinyardFtp, _vineyardUserName, _vineyardPassword);
			this.uploadFtp(vinyardOut, bbuFtp, _bbuUserName, _bbuPassword);
		}

		/* P3 positions */
		if (ftpAddress != null) {
			List<PortfolioElement> p3Portfolio = getP3Positions(inFile, vinyardFtp);
			exportCSVP3(p3Portfolio, P3Out);
			this.uploadFtp(P3Out, bbuFtp, _bbuUserName, _bbuPassword);

		}
	}

	private List<Stock> getStock() {
		List<Stock> stocks = new ArrayList<>();
		final String query = "SELECT * FROM clearing.dbo.ActiveSymbolList";

		try (Statement stmt = _conn.createStatement()) {

			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int j = 1;
				stocks.add(new Stock(rs.getString(j++) // symbol
				));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stocks;
	}

	private List<PortfolioElement> getPortfolioElement() {
		List<PortfolioElement> options = new ArrayList<>();
		final String query = "SELECT * from clearing.dbo.GSECPos_For_Blmbrg";
		// final String query = "SELECT * from clearing.dbo.GSECPos_For_Blmbrg " +
		// "where tradingsymbol in  (SELECT [Symbol] " +
		// "FROM [Clearing].[dbo].[AccountToMicrohedgeAccount_Map] "+
		// "where UserAccount = 'ENG' and Account = 'AHLX1209')";
		try (Statement stmt = _conn.createStatement()) {

			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int j = 1;
				if (!_nameCorrection.containsKey(rs.getString(1))) // only upload elements with tickers Bbg can understand
				{
					options.add(new PortfolioElement(correctName(rs.getString(j++)), // symbol
							rs.getDouble(j++), // tdQty
							ParseDate.standardFromDate(rs.getDate(j++)), // maturity
							rs.getDouble(j++), // strike
							rs.getString(j++), // putCall
							rs.getDouble(j++), // baseMarketPrice
							rs.getString(j++), // OCCCode
							rs.getString(j++), // account
							"vinyard"
							));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return options;
	}

	private String correctName(String name) {
		if (_nameCorrection.containsKey(name)) {
			return _nameCorrection.get(name);
		} else {
			return name;
		}
	}

	private void exportCSVGSEC(List<Stock> stocks, List<PortfolioElement> portfolioElements, String outFile) {
		String[] outFiles = outFile.split(";");
		try {
			/* stock */
			File f = new File(outFiles[0]);
			if (f.exists()) f.delete();
			FileWriter out = new FileWriter(outFiles[0]);
			// header
			out.append("Port,Symbol,Qty,Price,Date,Type\n");

			for (Stock s : stocks) {
				s.writeCSV(out);
			}
			out.flush();
			out.close();

			/* portfolio elements */
			f = new File(outFiles[1]);
			if (f.exists()) f.delete();
			out = new FileWriter(outFiles[1]);
			out.append("Port,Symbol,Qty,Price,Date,Type\n");

			for (PortfolioElement p : portfolioElements) {
				p.writeCSV(out);
			}
			out.flush();
			out.close();
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	private void exportCSVP3(List<PortfolioElement> p3Portfolio, String p3Out) {
		try (FileWriter out = new FileWriter(p3Out)) {
			File f = new File(p3Out);
			if (f.exists()) f.delete();

			out.append("Port,Symbol,Qty,Price,Date,Type\n");

			for (PortfolioElement p : p3Portfolio) {
				p.writeCSV(out);
			}
			out.flush();
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	private List<PortfolioElement> getP3Positions(String inFile, String address) throws Exception {
		String fileName = this.downloadFtp(inFile, address, _vineyardUserName, _vineyardPassword);
		List<PortfolioElement> positions = new ArrayList<>();

		try (BufferedReader in = new BufferedReader(new FileReader(fileName))) {
			String str;
			String[] lines;
			while ((str = in.readLine()) != null) {
				lines = str.split(",");
				simpleWrapper re = parseName(lines[4].trim());
				positions.add(new PortfolioElement(re.symbol.trim(), Double.parseDouble(lines[7].trim()), re.mat, re.strike, re.putCall, Double
						.parseDouble(lines[6].trim()), "", "", "P3"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return positions;
	}

	private simpleWrapper parseName(String name) throws Exception {
		simpleWrapper re = null;
		String[] str = name.split(" ");
		// stock
		if (str.length == 1) {
			re = new simpleWrapper(name, null, null, null);
		}
		// error
		else if (str.length != 4) {
			throw new Exception(String.format("BBU.parseName: invalid name(%s)", name));
		}
		// option
		else {
			String tmp = str[3];
			if (tmp.startsWith("MCP US 03/22/2014 C11.000")) {
				System.out.println("Hi");
			}
			re = new simpleWrapper(str[0].trim(), str[2].trim(), Double.parseDouble(tmp.substring(1).trim()), tmp.substring(0, 1).trim());
		}

		return re;
	}

	private class simpleWrapper {
		String	symbol, mat, putCall;
		Double	strike;

		public simpleWrapper(String sym, String maturity, Double k, String pc) {
			symbol = sym;
			mat = maturity;
			strike = k;
			putCall = pc;
		}
	}
}
