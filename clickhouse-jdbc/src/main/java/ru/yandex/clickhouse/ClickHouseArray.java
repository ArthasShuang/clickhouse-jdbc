package ru.yandex.clickhouse;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArray implements Array {
    private ClickHouseDataType elementType;
    private Object array;

    public ClickHouseArray(ClickHouseDataType elementType, Object array) {
        if (array == null) {
            throw new IllegalArgumentException("array cannot be null");
        }
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("not array");
        }
        this.elementType = elementType;
        this.array = array;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return elementType.name();
    }

    @Override
    public int getBaseType() throws SQLException {
        return elementType.getSqlType();
    }

    @Override
    public Object getArray() throws SQLException {
        if (array == null){
            throw new SQLException("Call after free");
        }
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) {
            throw new SQLFeatureNotSupportedException("The map is not empty!");
        }

        if (index < 1) {
            throw new SQLFeatureNotSupportedException(String.format("The array index is out of range: %d", index));
        }


        if (count == 0) {
            count = ((Object[]) array).length;
        }

        // array index out of range
        if ((--index) + count > ((Object[]) array).length) {
            throw new SQLFeatureNotSupportedException(String.format("The array index is out of range: %d, number of elements: %d.",
                            index + count, ((Object[]) array).length));
        }

        return subArray(array, (int) index, count);
    }

    private Object subArray(Object array, int index, int count) {
        Object[] transArray = (Object[]) array;
        Object[] resultArray = new Object[count];
        if (count >= 0) {System.arraycopy(transArray, index, resultArray, 0, count);}
        return resultArray;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        System.out.printf("getResultSet().array: %s, elementType: %s%n", array.toString(), elementType.toString());
        throw new SQLFeatureNotSupportedException(String.format("getResultSet().array: %s, elementType: %s%n", array.toString(), elementType.toString()));
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        array = null;
    }
}
