package at.enactmentengine.serverless.simulation;

import at.enactmentengine.serverless.exception.MissingResourceLinkException;
import at.enactmentengine.serverless.exception.RegionDetectionException;
import at.enactmentengine.serverless.nodes.Node;
import at.enactmentengine.serverless.object.Utils;
import at.enactmentengine.serverless.simulation.metadata.MetadataStore;
import at.uibk.dps.afcl.functions.objects.PropertyConstraint;
import at.uibk.dps.afcl.functions.objects.Service;
import com.google.gson.Gson;
import jFaaS.utils.PairResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ServiceSimulationModel {

    private static final Logger logger = LoggerFactory.getLogger(ServiceSimulationModel.class);

    private final Service service;
    private Integer typeId;
    private Integer providerId;
    private Integer lambdaRegionId;
    private Integer originallambdaRegionId;
    private Integer serviceRegionId;

    private ServiceSimulationModel(Service service) {
        this.service = service;
        Pair<Integer, Integer> serviceTypeInfo = MetadataStore.get().getServiceTypeInformation(service.getServiceType());
        typeId = serviceTypeInfo.getLeft();
        providerId = serviceTypeInfo.getRight();
        if (service.getServiceType().equalsIgnoreCase("download") ||
                service.getServiceType().equalsIgnoreCase("FILE_DL") ||
                service.getServiceType().equalsIgnoreCase("DT_REMOVE")) {
            serviceRegionId = MetadataStore.get().getRegionId(service.getSource());
        } else if (service.getServiceType().equalsIgnoreCase("upload") ||
                service.getServiceType().equalsIgnoreCase("FILE_UP") ||
                service.getServiceType().equalsIgnoreCase("UT_REMOVE")) {
            serviceRegionId = MetadataStore.get().getRegionId(service.getTarget());
        }
    }

    public ServiceSimulationModel(Integer lambdaRegionId, String newLambdaRegionName, Service service) {
        this(service);
        if (newLambdaRegionName == null) {
            this.lambdaRegionId = lambdaRegionId;
        } else {
            this.lambdaRegionId = MetadataStore.get().getRegionId(newLambdaRegionName);
        }
        this.originallambdaRegionId = lambdaRegionId;
    }

    /**
     * Gets all used services from properties and adds them to the list.
     */
    public static List<Service> getUsedServices(List<PropertyConstraint> properties, Node node) {
        List<Service> serviceList = new ArrayList<>();
        String resourceLink = null;
        try {
            resourceLink = Utils.getResourceLink(properties, node);
        } catch (MissingResourceLinkException e) {
            throw new RuntimeException(e);
        }
        for (PropertyConstraint property : properties) {
            if (property.getName() != null && property.getName().equals("service")) {
                serviceList.add(parseServiceString(property.getValue()));
            } else if (property.getServices() != null && !property.getServices().isEmpty()) {
                serviceList.addAll(property.getServices());
            }
        }

        for (Service s : serviceList) {
            replaceReference(s, resourceLink, node, serviceList);
        }

        return serviceList;
    }

    /**
     * Replaces references in the source and target of a service with the concrete region code.
     *
     * @param s            the service to replace the source and target
     * @param resourceLink the resource link of the function
     * @param node         the function node
     * @param services     the list of all services of the function node
     */
    private static void replaceReference(Service s, String resourceLink, Node node, List<Service> services) {
        if (s.getSource() != null && s.getSource().contains(".")) {
            s.setSource(getReplacedString(s.getSource(), resourceLink, node, services));
        }
        if (s.getTarget() != null && s.getTarget().contains(".")) {
            s.setTarget(getReplacedString(s.getTarget(), resourceLink, node, services));
        }
    }

    /**
     * Replaces the string with the referenced region.
     *
     * @param toReplace    the string that contains the reference
     * @param resourceLink the resource link of the function
     * @param node         the function node
     * @param services     the list of all services of the function node
     *
     * @return the replaced string containing the concrete region code
     */
    private static String getReplacedString(String toReplace, String resourceLink, Node node, List<Service> services) {
        // if the node is referenced, get the region of the function node
        if (toReplace.startsWith(node.getName() + ".")) {
            try {
                return Utils.detectRegion(resourceLink);
            } catch (RegionDetectionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // otherwise, another service is referenced
            Optional<Service> referencedService = services.stream()
                    .filter(service -> toReplace.startsWith(service.getName() + "."))
                    .findFirst();
            if (referencedService.isPresent()) {
                Service service = referencedService.get();
                if (service.getSource().contains(".") || service.getTarget().contains(".")) {
                    replaceReference(service, resourceLink, node, services);
                }

                String fieldToUse = toReplace.split("\\.")[1];
                if (fieldToUse.equalsIgnoreCase("source")) {
                    return service.getSource();
                } else if (fieldToUse.equalsIgnoreCase("target")) {
                    return service.getTarget();
                }
            }
        }
        throw new ServiceStringException("Referenced string " + toReplace + " does not match any node or service.");
    }

    /**
     * Parses the legacy service string to a service.
     *
     * @param serviceString the string to parse
     *
     * @return the created service object
     */
    private static Service parseServiceString(String serviceString) {
        Service s = new Service();
        List<String> properties = Arrays.asList(serviceString.split(":"));
        s.setServiceType(properties.get(0));
        // TODO other scenarios?
        if (s.getServiceType().equalsIgnoreCase("download") ||
                s.getServiceType().equalsIgnoreCase("FILE_DL") ||
                s.getServiceType().equalsIgnoreCase("DT_REMOVE")) {
            s.setSource(properties.get(1));
        } else if (s.getServiceType().equalsIgnoreCase("upload") ||
                s.getServiceType().equalsIgnoreCase("FILE_UP") ||
                s.getServiceType().equalsIgnoreCase("UT_REMOVE")) {
            s.setTarget(properties.get(1));
        }
        s.setAmountOfUnits(Integer.parseInt(properties.get(2)));
        s.setWorkPerUnit(Double.parseDouble(properties.get(3)));
        return s;
    }

    /**
     * Computes the total round trip time for all used services for the lambda region with the given name.
     *
     * @param lambdaRegionId      the id of the lambda region of the deployment
     * @param newLambdaRegionName the region name to simulate
     * @param usedServices        the list of services
     *
     * @return a pair containing the output of the services and their RTT
     */
    public static PairResult<String, Long> calculateTotalRttForUsedServices(Integer lambdaRegionId, String newLambdaRegionName, List<Service> usedServices) {
        long rtt = 0;
        int index = 1;

        Map<String, Double> serviceOutput = new HashMap<>();

        for (Service service : usedServices) {
            if (service.getServiceType().equalsIgnoreCase("compute"))
                continue;

            ServiceSimulationModel serviceSimulationModel = new ServiceSimulationModel(lambdaRegionId, newLambdaRegionName, service);
            rtt += serviceSimulationModel.calculateRTT(index, serviceOutput);
            index++;
        }

        return new PairResult<>(new Gson().toJson(serviceOutput), rtt);
    }

    /**
     * Calculates the service time and returns as negative number to be used to subtract.
     *
     * @param serviceRegion the region of the service to calculate
     *
     * @return the negative service time
     */
    private double subtractServiceTime(Integer serviceRegion) {
        Pair<Double, Double> dataTransferParams = MetadataStore.get().getDataTransferParamsFromDB(service.getServiceType(), lambdaRegionId,
                serviceRegion, originallambdaRegionId, true);
        Double bandwidth = dataTransferParams.getLeft();        // in Mbps
        Double latency = dataTransferParams.getRight();         // in ms

        return -(service.getAmountOfUnits() * latency + ((service.getWorkPerUnit() / bandwidth) * 1000));
    }

    /**
     * Calculates the simulated round trip time of a service deployment.
     *
     * @return simulated round trip time in milliseconds
     */
    public long calculateRTT(int index, Map<String, Double> output) {
        double roundTripTime;
        double subtractedTime = 0;
        // check if service is of type file transfer
        if (service.getServiceType().equals("download") || service.getServiceType().equals("FILE_DL") ||
                service.getServiceType().equals("upload") || service.getServiceType().equals("FILE_UP")) {
            Pair<Double, Double> dataTransferParams = MetadataStore.get().getDataTransferParamsFromDB(service.getServiceType(), lambdaRegionId,
                    serviceRegionId, originallambdaRegionId, false);
            Double bandwidth = dataTransferParams.getLeft();        // in Mbps
            Double latency = dataTransferParams.getRight();         // in ms

            roundTripTime = service.getAmountOfUnits() * latency + ((service.getWorkPerUnit() / bandwidth) * 1000);

            // if the download or upload is specified as service, the stored time has to be subtracted first
            if (service.getServiceType().equals("download") || service.getServiceType().equals("upload")) {
                if (service.getDbServiceRegion() != null) {
                    subtractedTime = subtractServiceTime(MetadataStore.get().getRegionId(service.getDbServiceRegion()));
                } else {
                    // use collocated
                    subtractedTime = subtractServiceTime(originallambdaRegionId);
                }
            }
        } else if (service.getServiceType().equals("DT_REMOVE") || service.getServiceType().equals("UT_REMOVE")) {
            roundTripTime = subtractServiceTime(serviceRegionId);
        } else {
            // 1. get missing information from DB
            // 1.1 get Networking information
            Triple<Double, Double, Double> networkParams = MetadataStore.get().getNetworkParamsFromDB(lambdaRegionId, serviceRegionId);
            Double bandwidth = networkParams.getLeft();
            Double lambdaLatency = networkParams.getMiddle();
            Double serviceLatency = networkParams.getRight();

            // 1.2 get Service Information
            Pair<Double, Double> serviceParams = MetadataStore.get().getServiceParamsFromDB(typeId, serviceRegionId);
            Double velocity = serviceParams.getLeft();
            Double startUpTime = serviceParams.getRight();

            // 2. calculate RTT according to model
            roundTripTime = (service.getAmountOfUnits() / velocity) + lambdaLatency +
                    startUpTime + (service.getWorkPerUnit() / bandwidth) + serviceLatency;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(service.getName() != null ? service.getName() + ":" : "")
                .append(service.getServiceType()).append(":")
                .append(service.getAmountOfUnits()).append(":")
                .append(service.getWorkPerUnit());

        Double serviceTime = ((double) Math.round(roundTripTime * 100)) / 100;
        output.put("service_" + index + "_" + sb, serviceTime);

        logger.info("Simulated service runtime of " + sb + ": " + serviceTime + "ms");

        roundTripTime += subtractedTime;

        return (long) roundTripTime;
    }

    public static class ServiceStringException extends RuntimeException {
        public ServiceStringException(String message) {
            super(message);
        }
    }

    public static class DatabaseEntryNotFoundException extends RuntimeException {
        public DatabaseEntryNotFoundException(String message) {
            super(message);
        }
    }
}
