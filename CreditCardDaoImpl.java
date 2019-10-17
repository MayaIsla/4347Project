package cs4347.jdbcProject.ecomm.dao.impl;

import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.util.DAOException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class CreditCardDaoImpl implements CreditCardDAO
{
	private static final String deleteSQL = "DELETE FROM creditcard WHERE id = ?;";
	private static final String countSQL = "SELECT COUNT(*) FROM creditcard;";
	private static final String retrieveCardIDSQL = "SELECT * FROM creditcard WHERE id = ?;";
	private static final String deleteCustomerSQL = "DELETE FROM creditcard WHERE Customer_id = ?;";
	private static final String updateCCSql = "UPDATE creditcard SET ccName = ?, ccNumber = ?, expDate = ?, securityCode = ? WHERE id = ?;";


	@Override
	    public CreditCard create(Connection connection, CreditCard creditCard, Long customerID)
	            throws SQLException, DAOException {
	        if (creditCard.getId() != null) {
	            throw new DAOException("Trying to insert CreditCard with NON-NULL ID");
	        }

	        PreparedStatement ps = null;
	        try {
	            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);

	            ps.setString(1, creditCard.getCcName());
	            ps.setString(2, creditCard.getCcNumber());
	            ps.setString(3, creditCard.getExpDate());
	            ps.setInt(4, creditCard.getSecurityCode());
	            ps.setLong(5, CustomerID);
	            ps.executeUpdate();

	            // Copy the assigned ID to the game instance.
	            ResultSet keyRS = ps.getGeneratedKeys();
	            keyRS.next();
	            int lastKey = keyRS.getInt(1);
	            creditCard.setId((long) lastKey);
	            return creditCard;
	        } finally {
	            if (ps != null && !ps.isClosed()) {
	                ps.close();
	            }
	        }
	    }

	    @Override
	    public CreditCard retrieve(Connection connection, Long ccID) throws SQLException, DAOException {
	        PreparedStatement preparedStatement = null;
	        CreditCard creditCard = null;

	        if (ccID == null) {
	            throw new DAOException("Trying to retrieve CreditCard with Null ID");
	        }

	        try {
	            preparedStatement = connection.prepareStatement(retrieveCardIDSQL);
	            preparedStatement.setLong(1, ccID);
	            ResultSet resultSet = preparedStatement.executeQuery();
	            if (resultSet.next()) {
	                creditCard = new CreditCard();
	                creditCard.setId(resultSet.getLong("id"));
	                creditCard.setCcName(resultSet.getString("ccName"));
	                creditCard.setSecurityCode(resultSet.getInt("securityCode"));
	                creditCard.setCcNumber(resultSet.getString("ccNumber"));
	                creditCard.setExpDate(resultSet.getString("expDate"));
	            }
	        } finally {
	            if (preparedStatement != null && !preparedStatement.isClosed()) {
	                preparedStatement.close();
	            }
	        }

	        return creditCard;
	    }

	    @Override
	    public List<CreditCard> retrieveCreditCardsForCustomer(Connection connection, Long customerID)
	            throws SQLException, DAOException {
	        PreparedStatement preparedStatement = null;
	        List<CreditCard> cardList = new ArrayList<>();

	        if (customerID == null) {
	            throw new DAOException("Trying to retrieve CreditCard with Null Customer ID");
	        }
	        try {
	            preparedStatement = connection.prepareStatement(retrieveCardSQL);
	            preparedStatement.setLong(1, customerID);
	            ResultSet resultSet = preparedStatement.executeQuery();
	            while (resultSet.next()) {
	                CreditCard card = new CreditCard();
	                card.setId(resultSet.getLong("id"));
	                card.setCcName(resultSet.getString("ccName"));
	                card.setCcNumber(resultSet.getString("ccNumber"));
	                card.setSecurityCode(resultSet.getInt("securityCode"));
	                card.setExpDate(resultSet.getString("expDate"));
	                cardList.add(card);
	            }
	        } finally {
	            if (preparedStatement != null && !preparedStatement.isClosed()) {
	                preparedStatement.close();
	            }
	        }

	        return cardList;
	    }

	    @Override
	    public int update(Connection connection, CreditCard creditCard) throws SQLException, DAOException {
	        PreparedStatement ps = null;
	        if (creditCard.getId() == null) {
	            throw new DAOException("Trying to update CreditCard with Null ID");
	        }
	        try {
	            ps = connection.prepareStatement(updateccSql);

	            ps.setString(1, creditCard.getCcName());
	            ps.setString(2, creditCard.getCcNumber());
	            ps.setString(3, creditCard.getExpDate());
	            ps.setInt(4, creditCard.getSecurityCode());
	            ps.setLong(5, creditCard.getId());
	            return ps.executeUpdate();

	        } finally {
	            if (ps != null && !ps.isClosed()) {
	                ps.close();
	            }
	        }
	    }

	    @Override
	    public int delete(Connection connection, Long ccID) throws SQLException, DAOException {
	        PreparedStatement ps = null;
	        if (ccID == null) {
	            throw new DAOException("Trying to delete CreditCard with Null ID");
	        }
	        try {
	            ps = connection.prepareStatement(deleteSql);

	            ps.setLong(1, ccID);
	            return ps.executeUpdate();

	        } finally {
	            if (ps != null && !ps.isClosed()) {
	                ps.close();
	            }
	        }
	    }

	    @Override
	    public int deleteForCustomer(Connection connection, Long customerID) throws SQLException, DAOException {
	        PreparedStatement ps = null;
	        if (customerID == null) {
	            throw new DAOException("Trying to retrieve Credit Card with Null Customer ID");
	        }
	        try {
	            ps = connection.prepareStatement(deleteCustomerSQL);

	            ps.setLong(1, customerID);
	            return ps.executeUpdate();

	        } finally {
	            if (ps != null && !ps.isClosed()) {
	                ps.close();
	            }
	        }
	    }

	    @Override
	    public int count(Connection connection) throws SQLException, DAOException {
	        PreparedStatement preparedStatement = null;
	        try {
	            preparedStatement = connection.prepareStatement(countSql);
	            ResultSet resultSet = preparedStatement.executeQuery();
	            if (resultSet.next()) {
	                return resultSet.getInt(1);
	            }

	        } finally {
	            if (preparedStatement != null && !preparedStatement.isClosed()) {
	                preparedStatement.close();
	            }
	        }

	        return 0;
	    }

}
