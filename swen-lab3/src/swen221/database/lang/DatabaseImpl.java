package swen221.database.lang;

import java.util.ArrayList;
import java.util.List;

public class DatabaseImpl implements Database{

    private final ColumnType[] schema;
    private final List<Object[]> rows;
    private final int keyField;

    public DatabaseImpl(ColumnType[] schema, List<Object[]> rows, int keyField) {
        this.schema = schema;
        this.rows = rows;
        this.keyField = keyField;
    }

    @Override
    public ColumnType[] getSchema() {
        return schema;
    }

    @Override
    public int getKeyField() {
        return keyField;
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public void addRow(Object... data) throws InvalidRowException, DuplicateKeyException {

        if (data.length != 2)
            throw new InvalidRowException();

        //Check data is the correct types
        for (int i = 0; i < data.length; i++) {
           Object fieldType = data[i];
           RowType schemaType = schema[i].getType();

           if (!schemaType.isInstance(fieldType))
               throw new InvalidRowException();
        }

        //Check data is not a duplicate
        Object key = data[keyField];
        for (Object[] row : rows){
            if (row[keyField].equals(key))
                throw new DuplicateKeyException();
        }
        rows.add(data);
    }

    @Override
    public Object[] getRow(Object key) throws InvalidKeyException {

        //Gets the key value pair from rows
        for (Object[] pair : rows){
            Object keyfield = pair[keyField];
            if (keyfield.equals(key))
                return pair;
        }
        throw new InvalidKeyException();
    }

    @Override
    public Object[] getRow(int index) throws IndexOutOfBoundsException {
        return rows.get(index);
    }
}
