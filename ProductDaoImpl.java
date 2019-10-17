package cs4347.jdbcProject.ecomm.dao.impl;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;

public class ProductDaoImpl implements ProductDAO
{
	private static final String insertSQL =
            "INSERT INTO Product (Customer_id, Product_id, purchaseDate, purchasePrice) VALUES (?, ?, ?, ?);";

 @Override
    public Product create(Connection connection, Product product) throws SQLException, DAOException {
        if (Product.getId() != null) {
            throw new DAOException("Trying to insert product with NON-NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, product.getCustomerID());
            ps.setFloat(4, product.getPurchasePrice());
            ps.setLong(2, product.getProductID());
            ps.setDate(3, new java.sql.Date(Product.getPurchaseDate().getTime()));


            ps.executeUpdate();
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            Product.setId((long) lastKey);
            return Product;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String selectQuery = "SELECT id, Customer_id, Product_id, purchaseDate, purchasePrice FROM Product WHERE id = ?";

    @Override
    public Product retrieveID(Connection connection, Long ProductID) throws SQLException, DAOException {
        if (ProductID == null) {
            throw new DAOException("Trying to retrieve Product with NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectQuery);
            ps.setLong(1, ProductID);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }

            Product cust = extractFromRS(rs);
            return cust;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String selectCustomerProductQuery = "SELECT id, Customer_id, Product_id, purchaseDate, purchasePrice FROM Product WHERE Customer_id = ? AND Product_id = ? ";

    @Override
    public Product retrieveCustomerProductID(Connection connection, Long Customer_id, Long Product_id)
            throws SQLException, DAOException {
        if (Customer_id == null || Product_id == null) {
            throw new DAOException("Trying to retrieve Product with NULL Customer_id or Product_id");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectCustomerProductQuery);
            ps.setLong(1, Customer_id);
            ps.setLong(2, Product_id);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }

            Product cust = extractFromRS(rs);
            return cust;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String storeProductQuery =
            "SELECT id, Customer_id, Product_id, purchaseDate, purchasePrice FROM Product WHERE Product_id = ?";

    @Override
    public List<Product> retrieveByProduct(Connection connection, Long Product_id) throws SQLException, DAOException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(storeProductQuery);
            ps.setLong(1, Product_id);
            ResultSet rs = ps.executeQuery();

            List<Product> result = new ArrayList<Product>();
            while (rs.next()) {
                Product cust = extractFromRS(rs);
                result.add(cust);
            }
            return result;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String storeCustomerQuery =
            "SELECT id, Customer_id, Product_id, purchaseDate, purchasePrice FROM Product WHERE Customer_id = ?";

    @Override
    public List<Product> retrieveByCustomer(Connection connection, Long Customer_id) throws SQLException, DAOException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(storeCustomerQuery);
            ps.setLong(1, Customer_id);
            ResultSet rs = ps.executeQuery();

            List<Product> result = new ArrayList<Product>();
            while (rs.next()) {
                Product cust = extractFromRS(rs);
                result.add(cust);
            }
            return result;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String updateSQL = "UPDATE Product SET Customer_id = ?, Product_id = ?, purchaseDate = ?, purchasePrice = ? WHERE id = ?;";

    @Override
    public int update(Connection connection, Product Product) throws SQLException, DAOException {
        if (Product.getId() == null) {
            throw new DAOException("Trying to update Product with NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
            ps.setLong(1, Product.getCustomerID());
            ps.setLong(2, Product.getProductID());
            ps.setDate(3, new java.sql.Date(Product.getPurchaseDate().getTime()));
            ps.setFloat(4, Product.getPurchasePrice());
            ps.setLong(5, Product.getId());

            int rows = ps.executeUpdate();
            return rows;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String deleteSQL = "DELETE FROM Product WHERE id = ?;";

    @Override
    public int delete(Connection connection, Long ProductOwnedID) throws SQLException, DAOException {
        if (ProductOwnedID == null) {
            throw new DAOException("Trying to delete Product with NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, ProductOwnedID);

            int rows = ps.executeUpdate();
            return rows;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String countSQL = "select count(*) from Product";

    @Override
    public int count(Connection connection) throws SQLException, DAOException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(countSQL);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new DAOException("No Count Returned");
            }
            int count = rs.getInt(1);
            return count;
        } finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    private Product extractFromRS(ResultSet rs) throws SQLException {
        Product cust = new Product();
        cust.setId(rs.getLong("id"));
        cust.setCustomerID(rs.getLong("Customer_id"));
        cust.setProductID(rs.getLong("Product_id"));
        cust.setPurchaseDate(rs.getDate("purchaseDate"));
        cust.setPurchasePrice(rs.getFloat("purchasePrice"));
        return cust;
    }

}
