package at.enactmentengine.serverless.Simulation;

import at.uibk.dps.databases.MariaDBAccess;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceSimulationModel {

    private final String type;

    private final double expectedWork;

    private final double expectedData;

    private Integer typeId;

    private Integer providerId;

    private Integer lambdaRegionId;
    private Integer serviceRegionId;

    private static final Logger logger = LoggerFactory.getLogger(ServiceSimulationModel.class);

    private ServiceSimulationModel(String serviceString){
        try {
            List<String> properties = Arrays.asList(serviceString.split(":"));
            type = properties.get(0);
            String serviceRegionName = properties.get(1);
            expectedWork = Double.parseDouble(properties.get(2));
            expectedData = Double.parseDouble(properties.get(3));
            Pair<Integer, Integer> serviceTypeInfo = getServiceTypeInformation();
            typeId = serviceTypeInfo.getLeft();
            providerId = serviceTypeInfo.getRight();
            this.serviceRegionId = getRegionId(serviceRegionName);
        } catch (RuntimeException e) {
            throw new ServiceStringException("Service deployment string could not be parsed.");
        }
    }

    public ServiceSimulationModel(Integer lambdaRegionId, String serviceString){
        this(serviceString);
        this.lambdaRegionId = lambdaRegionId;
    }

    public ServiceSimulationModel(String lambdaRegionName, String serviceString){
        this(serviceString);
        this.lambdaRegionId = getRegionId(lambdaRegionName);
    }

    /**
     * Calculates the simulated round trip time of a service deployment.
     * @return simulated round trip time in seconds
     */
    public long calculateRTT(){
        // 1. get missing information from DB
        // 1.1 get Networking information
        Triple<Double, Double, Double> networkParams = getNetworkParamsFromDB();
        Double bandwidth = networkParams.getLeft();
        Double lambdaLatency = networkParams.getMiddle();
        Double serviceLatency = networkParams.getRight();

        // 1.2 get Service Information
        Pair<Double, Double> serviceParams = getServiceParamsFromDB();
        Double velocity = serviceParams.getLeft();
        Double startUpTime = serviceParams.getRight();

        // 2. calculate RTT according to model
        double roundTripTime = (expectedWork / velocity) + lambdaLatency +
                startUpTime + (expectedData / bandwidth) + serviceLatency;
        logger.info("Simulated service runtime of " + type + ": "+ ((double) Math.round(roundTripTime * 100)) / 100 + "ms");
        return (long) roundTripTime;
    }

    /**
     * Fetches simulation relevant network parameters from the database.
     * @return triple of bandwidth [Mb/ms], lambda latency [ms] and service latency [ms].
     */
    private Triple<Double, Double, Double> getNetworkParamsFromDB(){
        // Choose which parameters to return according to service type
        // example: FaceRec vs Bucket

        Connection connection = MariaDBAccess.getConnection();
        String query = "SELECT bandwidth / 1000, latency AS latency FROM networking WHERE sourceRegionID = ? AND destinationRegionID = ?";
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, lambdaRegionId);
            preparedStatement.setInt(2, serviceRegionId);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            double bandwidth = resultSet.getDouble(1);
            double latency = resultSet.getDouble(2);
            if (serviceRegionId.equals(lambdaRegionId)) {
                return Triple.of(bandwidth, latency, latency);
            } else {
                preparedStatement.setInt(1, serviceRegionId);
                resultSet = preparedStatement.executeQuery();
                resultSet.next();
                double serviceLatency = resultSet.getDouble(2);
                return Triple.of(bandwidth, latency, serviceLatency);
            }

        } catch (SQLException e) {
            throw new DatabaseEntryNotFoundException("Could not fetch network parameters from database for " + lambdaRegionId + "," + serviceRegionId);
        }
    }

    /**
     * Fetches the service parameters from the database.
     * @return pair of relevant service parameters (velocity [ms/work], startup time [ms])
     */
    private Pair<Double, Double> getServiceParamsFromDB(){
        Connection connection = MariaDBAccess.getConnection();
        String query = "SELECT velocity, startup AS startup FROM serviceDeployment WHERE serviceID = ? AND regionID = ?";
        ResultSet resultSet = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, typeId);
            preparedStatement.setInt(2, serviceRegionId);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            return Pair.of(resultSet.getDouble(1), resultSet.getDouble(2));
        } catch (SQLException e) {
            throw new DatabaseEntryNotFoundException("Could not fetch service parameters from database");
        }
    }

    /**
     * Fetches the service type information from the database.
     * @return pair of (service-) typeId, providerId
     */
    private Pair<Integer, Integer> getServiceTypeInformation(){
        Connection connection = MariaDBAccess.getConnection();
        String query = "SELECT id, providerID FROM service WHERE type = ?";
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, type);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            return Pair.of(resultSet.getInt(1), resultSet.getInt(2));
        } catch (SQLException e) {
            throw new DatabaseEntryNotFoundException("Could not fetch service type information from database");
        }
    }

    /**
     * Fetches the regionId for a region with the specified name
     * @param regionName name of the region to fetch the id for
     * @return the corresponding regionId
     */
    private int getRegionId(String regionName) {
        Connection connection = MariaDBAccess.getConnection();
        String query = "SELECT id FROM region WHERE region = ? AND providerID = ?";
        ResultSet resultSet = null;

        try{
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, regionName);
            preparedStatement.setString(2, providerId.toString());
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e){
            throw new DatabaseEntryNotFoundException("No regionID could be determined from regionName '" + regionName + "'");
        }
    }

    public static class ServiceStringException extends RuntimeException {
        public ServiceStringException(String message){
            super(message);
        }
    }

    public static class DatabaseEntryNotFoundException extends RuntimeException{
        public DatabaseEntryNotFoundException (String message){
            super(message);
        }
    }
}
