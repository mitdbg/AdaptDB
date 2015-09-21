package core.index.key;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Ints;

import core.utils.TypeUtils.TYPE;
import core.utils.TypeUtilsMT;

public class CartilageIndexKeyMT extends CartilageIndexKey {

	public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	protected TypeUtilsMT typeUtils = new TypeUtilsMT();

	public CartilageIndexKeyMT(char delimiter) {
		super(delimiter);
	}

	public CartilageIndexKeyMT(char delimiter, int[] keyAttrIdx) {
		super(delimiter, keyAttrIdx);
	}

	public CartilageIndexKeyMT clone() throws CloneNotSupportedException {
		CartilageIndexKeyMT k = (CartilageIndexKeyMT) super.clone();
		k.typeUtils = new TypeUtilsMT();
		return k;
	}

	/**
	 * Extract the types of the relevant attributes (which need to be used as
	 * keys)
	 *
	 * @return
	 */
	public TYPE[] detectTypes(boolean skipNonKey) {
		List<TYPE> types = new ArrayList<TYPE>();

		numAttrs = 0;
		String[] tokens = new String(bytes, offset, length).split("\\"
				+ delimiter);
		for (int i = 0; i < tokens.length; i++) {
			String t = tokens[i].trim();
			if (t.equals(""))
				continue;

			numAttrs++;

			if (skipNonKey && keyAttrIdx != null
					&& !Ints.contains(keyAttrIdx, i))
				continue;

			if (typeUtils.isInt(t))
				types.add(TYPE.INT);
			else if (typeUtils.isLong(t))
				types.add(TYPE.LONG);
			else if (typeUtils.isFloat(t))
				types.add(TYPE.DOUBLE);
			else if (typeUtils.isDate(t, sdf))
				types.add(TYPE.DATE);
			else
				types.add(TYPE.STRING);

		}

		if (keyAttrIdx == null) {
			keyAttrIdx = new int[types.size()];
			for (int i = 0; i < keyAttrIdx.length; i++)
				keyAttrIdx[i] = i;
		}

		TYPE[] typeArr = new TYPE[types.size()];
		for (int i = 0; i < types.size(); i++)
			typeArr[i] = types.get(i);

		return typeArr;
	}
}
