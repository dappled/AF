package com.dong.dataWrapper;

import com.dong.utils.poi.PoiRecord;

/**
 * @author Zhenghong Dong
 */
public abstract class RecordAbstract implements Comparable<RecordAbstract>, PoiRecord {
	private final String	_symbol;	// symbol
	private final String	_type;		// base product: equity or option or futfop(1355 line in MoreImports.vb,
										// GSECPosition) or bond
	private final String	_side;		// different for different implementation: for Activity: B/S; for
										// Position:C/P/null(equity)
	private int				_qty;		// quantity

	/***********************************************************************
	 * Constructor
	 ***********************************************************************/
	public RecordAbstract(final String symbol, final String type, final String side, final int qty) {
		_symbol = symbol;
		_type = type;
		_side = side;
		_qty = qty;
	}

	/***********************************************************************
	 * Comparable methods
	 ***********************************************************************/
	@Override
	public int compareTo(final RecordAbstract o2) {
		int ret = getSymbol().compareTo( o2.getSymbol() );
		if (ret == 0) { return getType().compareTo( o2.getType() ); }
		return ret;
	}

	/***********************************************************************
	 * Object methods
	 ***********************************************************************/
	@Override
	public boolean equals(final Object record) {
		if (record == null) return false;
		if (record == this) return true;
		if (!(record instanceof ActivityAbstract)) return false;
		return (_symbol.equals( ((ActivityAbstract) record).getSymbol() ) && // compare symbol

				_type.equals( ((ActivityAbstract) record).getSide() ) && // compare side
		_qty == ((ActivityAbstract) record).getQuantity()); // compare quantity

	}

	@Override
	public int hashCode() {
		return _symbol.hashCode() * 31 + _qty * 53 + _type.hashCode() * 23;
	}

	/***********************************************************************
	 * Utilities
	 ***********************************************************************/

	/***********************************************************************
	 * Getter and Setter
	 ***********************************************************************/

	public String getSymbol() {
		return _symbol;
	}

	public int getQuantity() {
		return _qty;
	}

	public void setQuantity(final int qty) {
		_qty = qty;
	}

	public String getType() {
		return _type;
	}

	public String getSide() {
		return _side;
	}
}