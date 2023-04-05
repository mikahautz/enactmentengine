package at.enactmentengine.serverless.nodes;

import at.enactmentengine.serverless.exception.MissingInputDataException;
import at.enactmentengine.serverless.object.State;
import at.enactmentengine.serverless.parser.ElementIndex;
import at.uibk.dps.afcl.functions.objects.DataIns;
import at.uibk.dps.afcl.functions.objects.LoopCounter;
import at.uibk.dps.afcl.functions.objects.PropertyConstraint;
import com.google.gson.*;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Control node which manages the tasks at the start of a parallel for loop.
 *
 * @author markusmoosbrugger, jakobnoeckl adapted by @author stefanpedratscher
 */
public class ParallelForStartNode extends Node {

    /**
     * Logger for parallel-for-start node.
     */
    static final Logger logger = LoggerFactory.getLogger(ParallelForStartNode.class);
    /**
     * The properties of a parallel-for-start node.
     */
    List<PropertyConstraint> properties;
    /**
     * The constraints of a parallel-for-start node.
     */
    List<PropertyConstraint> constraints;
    /**
     * Input data defined in the workflow file.
     */
    private List<DataIns> dataIns;
    /**
     * The actual values of the counter variables.
     */
    private Map<String, Object> counterValues;
    /**
     * The start value of the loop counter.
     */
    private int counterStart;
    /**
     * The end value of the loop counter.
     */
    private int counterEnd;
    /**
     * The step size for the loop counter.
     */
    private int counterStepSize;
    /**
     * Contains all counter variable names.
     */
    private String[] counterVariableNames;
    /**
     * The maximum number of concurrent function executions.
     */
    private int maxNumberThreads = 1000;

    /**
     * Default constructor for the parallel-for-start node.
     *
     * @param name        of the parallel-for-start node.
     * @param type        of the parallel-for-start node.
     * @param dataIns     specified in the workflow file.
     * @param loopCounter of the parallel-for-start node.
     */
    public ParallelForStartNode(String name, String type, List<DataIns> dataIns, LoopCounter loopCounter, List<PropertyConstraint> properties,
                                List<PropertyConstraint> constraints) {
        super(name, type);
        this.dataIns = dataIns;
        this.properties = properties;
        this.constraints = constraints;
        counterVariableNames = new String[3];
        parseLoopCounter(loopCounter);

        checkConstraints(this.constraints);
    }

    /**
     * Check the constraints of the parallel-for-start node.
     *
     * @param constraints of the parallel-for-start node.
     */
    private void checkConstraints(List<PropertyConstraint> constraints) {

        /* Check if there are constraints specified */
        if (constraints != null) {
            for (PropertyConstraint constraint : constraints) {

                /* Check for concurrency constraint */
                if ("concurrency".equals(constraint.getName())) {
                    try {
                        maxNumberThreads = Integer.parseInt(constraint.getValue());
                        logger.info("Detected new concurrency for " + name + ": " + maxNumberThreads);
                    } catch (java.lang.NumberFormatException e) {
                        logger.warn("Could not parse new concurrency. Default concurrency value is used: " + maxNumberThreads);
                    }
                }
            }
        }
    }

    /**
     * Parses the loop counter. Tries to cast each value as integer. If casting as integer is not possible it assumes
     * that the value comes from a variable.
     *
     * @param loopCounter loopCounter
     */
    private void parseLoopCounter(LoopCounter loopCounter) {

        /* Try to parse the from value of the loop counter */
        try {
            counterStart = Integer.valueOf(loopCounter.getFrom());
        } catch (NumberFormatException e) {
            counterVariableNames[0] = loopCounter.getFrom();
        }

        /* Try to parse the to value of the loop counter */
        try {
            counterEnd = Integer.valueOf(loopCounter.getTo());
        } catch (NumberFormatException e) {
            counterVariableNames[1] = loopCounter.getTo();
        }

        /* Try to parse the step value of the loop counter */
        try {
            counterStepSize = Integer.valueOf(loopCounter.getStep());
        } catch (NumberFormatException e) {

            // TODO should string be allowed for dynamic variables?
            counterStepSize = 1;
        }
    }

    /**
     * Saves the passed result as dataValues.
     *
     * @param input values to pass.
     */
    @Override
    public void passResult(Map<String, Object> input) {
        synchronized (this) {

            /* Prepare data value holders, if not already done */
            if (dataValues == null) {
                dataValues = new HashMap<>();
            }
            if (counterValues == null) {
                counterValues = new HashMap<>();
            }

            /* Check if there is an input specified */
            if (dataIns != null) {

                /* Iterate over inputs and add corresponding values to the data values */
                for (DataIns data : dataIns) {
                    if (input.containsKey(data.getSource())) {
                        dataValues.put(data.getSource(), input.get(data.getSource()));
                    }
                }
            }

            /* Iterate over counter variables and check if the input contains the values */
            for (String counterValue : counterVariableNames) {
                if (input.containsKey(counterValue)) {
                    counterValues.put(counterValue, input.get(counterValue));
                }
            }
        }
    }

    /**
     * Checks the input values, adds specific number of children depending on the input values and creates a thread pool
     * for execution of the children.
     *
     * @return True on success, False otherwise
     *
     * @throws Exception on failure
     */
    @Override
    public Boolean call() throws Exception {

        /* Prepare the output values */
        final Map<String, Object> outValues = new HashMap<>();

        synchronized (this) {

            /* Prepare data value holders, if not already done */
            if (dataValues == null) {
                dataValues = new HashMap<>();
            }
            if (counterValues == null) {
                counterValues = new HashMap<>();
            }

            /* Check if there is an input specified */
            if (dataIns != null) {

                /* Iterate over inputs and add corresponding values to the data values */
                for (DataIns data : dataIns) {
                    String dataSources;
                    if (this.getId() != 0) {
                        dataSources = data.getSource() + "/" + this.getId();
                    } else if (!parents.isEmpty() && parents.get(0).getId() != 0 && parents.get(0).getClass() != ParallelForEndNode.class) {
                        dataSources = data.getSource() + "/" + parents.get(0).getId();
                    } else {
                        dataSources = data.getSource();
                    }

                    String source = dataSources.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]", "");
                    String[] sourceList = source.split(",");

                    for (String dataSource : sourceList) {

                        String subObject = null;

                        long count = dataSource.chars().filter(ch -> ch == '/').count();
                        if (count > 1) {
                            subObject = State.getInstance().findJSONSubObject(dataSource.substring(0, dataSource.indexOf("/", dataSource.indexOf("/") + 1)), dataSource.substring(dataSource.indexOf("/", dataSource.indexOf("/") + 1) + 1), count);
                        }

                        if (State.getInstance().getStateObject().get(dataSource) != null || subObject != null) {
                            String toUse = null;
                            if (data.getType().equals("collection") || (State.getInstance().getStateObject().get(dataSource) instanceof JsonArray)) {
                                toUse = subObject != null ? subObject : State.getInstance().getStateObject().get(dataSource).toString();
                            } else {
                                toUse = subObject != null ? subObject : State.getInstance().getStateObject().get(dataSource).getAsString();
                            }

                            dataValues.put(dataSource, toUse);
                        }
                    }
                }
            }

            /* Iterate over counter variables and check if the input contains the values */
            for (String counterValue : counterVariableNames) {
                if(counterValue == null){
                    continue;
                }

                String source = counterValue.replace("\\s+","").replace("\\[", "").replace("\\]", "");
                String[] sourceList = source.split(",");

                for(String counterSource : sourceList){
                    if (State.getInstance().getStateObject().get(counterSource) != null) {
                        counterValues.put(counterSource, State.getInstance().getStateObject().get(counterSource));
                    }
                }
            }
        }

        /* Check if there is input defined in the workflow file */
 /*       if (dataIns != null) { */



            /* Iterate over the input data and handle input values */
            /*for (DataIns data : dataIns) {
                if (State.getInstance().getStateObject().get(data.getSource()) == null) {
                    throw new MissingInputDataException(ParallelForStartNode.class.getCanonicalName() + ": " + name
                            + " needs " + data.getSource() + "!");
                } else {
                    State.getInstance().getStateObject().add(name + "/" + data.getName(), new Gson().fromJson(State.getInstance().getStateObject().get(data.getSource()).toString(), JsonElement.class));
                    outValues.put(name + "/" + data.getName(), State.getInstance().getStateObject().get(data.getSource()));
                }
            }
        }
       */

        logger.info("Executing {} ParallelForStartNodeOld", name);

        /* Create all children functions (all functions inside the parallel-for) */
        addChildren();

        /* Create a fixed thread-pool managing the parallel executions */
        ExecutorService exec = Executors
                .newFixedThreadPool(children.size() > maxNumberThreads ? maxNumberThreads : children.size());
        List<Future<Boolean>> futures = new ArrayList<>();
        List<Map<String, Object>> outValuesForChildren = transferOutVals(children.size(), outValues);

        int customConcurrencyLimit = maxNumberThreads == 1000 ? -1 : maxNumberThreads;

        /* Iterate over all children */
        for (int i = 0; i < children.size(); i++) {

            Node node = children.get(i);
            node.setLoopCounter(i);
            node.setMaxLoopCounter(counterEnd - 1);
            node.setConcurrencyLimit(customConcurrencyLimit);
            node.setStartTime(startTime);
            // if another construct is following directly afterwards, set the field to 0 (needed if concurrency limit is exceeded)
            if (node instanceof IfStartNode) {
                ((IfStartNode) node).isAfterParallelForNode = 0;
            } else if (node instanceof ParallelStartNode) {
                ((ParallelStartNode) node).isAfterParallelForNode = 0;
            } else if (node instanceof SwitchStartNode) {
                ((SwitchStartNode) node).isAfterParallelForNode = 0;
            }

            node.setId(i + 1);
            setIdOfChildren(node, i + 1);

            /* Pass results to the children (if there is an output value left) */
            if (i < outValuesForChildren.size()) {
                for(String temp : outValuesForChildren.get(i).keySet()){
                    State.getInstance().getStateObject().addProperty(temp + "/" + (i + 1), outValuesForChildren.get(i).get(temp).toString());
                }
                //node.passResult(outValuesForChildren.get(i));
            }

            /* Execute the child node */
            futures.add(exec.submit(node));
        }

        /* Wait for all children to finish */
        for (Future<Boolean> future : futures) {
            future.get();
        }

        /* Terminate executor */
        exec.shutdown();

        return true;
    }

    /**
     *
     * @param node Node for which the id's of the children are set
     * @param id The id of the iteration
     */
    private void setIdOfChildren(Node node, int id){
        if(node.getClass() == ParallelForEndNode.class){
            return;
        }

        for(int j = 0; j < node.getChildren().size(); j++){
            node.getChildren().get(j).setId(id);
            setIdOfChildren(node.getChildren().get(j), id);
        }
    }

    /**
     * Adds a specific number of children depending on the values counterStart,
     * counterEnd and counterStepSize.
     *
     * @throws MissingInputDataException  on missing input.
     * @throws CloneNotSupportedException on unsupported clone.
     */
    private void addChildren() throws MissingInputDataException, CloneNotSupportedException {

        /* Iterate over counter variables and check if there is the according value */
        for (int i = 0; i < counterVariableNames.length; i++) {

            String counterKeyName = counterVariableNames[i];

            boolean gotDataFromDataSource = false;

            if(counterKeyName == null){
                continue;
            }

            String source = counterKeyName.replaceAll("\\s+","").replaceAll("\\[", "").replaceAll("\\]", "");;
            String[] sourceList = source.split(",");

            for(String counterSource : sourceList){
                if (counterValues.containsKey(counterSource)) {
                    counterVariableNames[i] = counterSource;
                    gotDataFromDataSource = true;
                }
            }

            if(!gotDataFromDataSource){
                throw new MissingInputDataException(
                        ParallelForStartNode.class.getCanonicalName() + ": " + name + " needs " + counterKeyName + "!");
            }
        }

        // TODO could counterStart, counterEnd and counterStepSize be of type NUMBER?

        /* Parse actual value of the defined variables */
        if (counterVariableNames[0] != null) {
            counterStart = Integer.parseInt((String) counterValues.get(counterVariableNames[0]));
        }
        if (counterVariableNames[1] != null) {
            counterEnd = ((int) Double.parseDouble(counterValues.get(counterVariableNames[1]).toString()));
        }
        if (counterVariableNames[2] != null) {
            counterStepSize = Integer.parseInt((String) counterValues.get(counterVariableNames[2]));
        }

        logger.info("Counter values for " + ParallelForStartNode.class.getCanonicalName() + " : " +
                "counterStart: " + counterStart + ", counterEnd: " + counterEnd + ", stepSize: " + counterStepSize + "");

        /* Search the end node of the parallel-for */
        ParallelForEndNode endNode = findParallelForEndNode(children.get(0), 0);

        /* Add children to the list of children */
        for (int i = counterStart; i < counterEnd - 1; i += counterStepSize) {
            Node node = children.get(0).clone(endNode);
            children.add(node);
        }

        assert endNode != null;

        /* Set the number all children in the parallel-for */
        endNode.setNumberOfParents(children.size());
    }

    /**
     * Finds the matching ParallelForEndNodeOld recursively.
     *
     * @param currentNode node to start looking for.
     * @param depth       recursive depth to check which parallel-for node is checked.
     *
     * @return the end node of the parallel-for.
     */
    private ParallelForEndNode findParallelForEndNode(Node currentNode, int depth) {

        /* Iterate over all children */
        for (Node child : currentNode.getChildren()) {
            if (child instanceof ParallelForEndNode) {

                /* Check if we found the end node of the correct parallel-for node */
                if (depth == 0) {
                    return (ParallelForEndNode) child;
                } else {

                    /* We found an end node of a nested parallel-for */
                    return findParallelForEndNode(child, depth - 1);
                }
            } else if (child instanceof ParallelForStartNode) {

                /* We found the start node of another parallel-for */
                return findParallelForEndNode(child, depth + 1);
            } else {
                return findParallelForEndNode(child, depth);
            }
        }
        return null;
    }

    /**
     * Transfers the output values depending on the specified dataFlow type.
     *
     * @param children  The number of children.
     * @param outValues The output values.
     *
     * @return the transferred output values.
     */
    private ArrayList<Map<String, Object>> transferOutVals(int children, Map<String, Object> outValues) {

        ArrayList<Map<String, Object>> values = new ArrayList<>();

        /* Check if there is an input defined */
        if (dataIns != null) {

            /* Iterate over the input data defined in the workflow file */
            for (DataIns data : dataIns) {

                /* Check of there are constraints defined */
                if (data.getConstraints() != null) {
                    String dataSources;
                    if (this.getId() != 0) {
                        dataSources = data.getSource() + "/" + this.getId();
                    } else if (!parents.isEmpty() && parents.get(0).getId() != 0 && parents.get(0).getClass() != ParallelForEndNode.class) {
                        dataSources = data.getSource() + "/" + parents.get(0).getId();
                    } else {
                        dataSources = data.getSource();
                    }

                    String source = dataSources.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]", "");
                    String[] sourceList = source.split(",");

                    for (String dataSource : sourceList) {

                        if (dataValues.get(dataSource) == null) {
                            continue;
                        }

                        /* Check if the actual input is an array */
                        if (dataValues.get(dataSource) instanceof ArrayList || dataValues.get(dataSource) instanceof JsonArray) {

                            /* Convert the data to an array */
                            JsonArray dataElements = new Gson().toJsonTree(dataValues.get(dataSource)).getAsJsonArray();

                            /* Check if a distribution is specified */
                            List<JsonArray> distributedElements = distributeElements(dataElements, data.getConstraints(), children);
                            checkDistributedElements(distributedElements, data, values);
                        } else {

                            // TODO can the following be simplified and generalized e.g. also for bool etc.?
                            if (dataValues.get(dataSource) instanceof Double){
                                JsonArray dataElements = new JsonArray();
                                dataElements.add((Double) dataValues.get(dataSource));
                                List<JsonArray> distributedElements = distributeElements(dataElements, data.getConstraints(), children);

                                checkDistributedElements(distributedElements, data, values);
                            } else if (dataValues.get(dataSource) instanceof Integer){
                                JsonArray dataElements = new JsonArray();
                                dataElements.add((Integer) dataValues.get(dataSource));
                                List<JsonArray> distributedElements = distributeElements(dataElements, data.getConstraints(), children);

                                checkDistributedElements(distributedElements, data, values);
                            } else if (dataValues.get(dataSource) instanceof String){
                                JsonArray dataElements = new JsonArray();
                                dataElements.add((String) dataValues.get(dataSource));
                                List<JsonArray> distributedElements = distributeElements(dataElements, data.getConstraints(), children);

                                checkDistributedElements(distributedElements, data, values);
                            } else if (dataValues.get(dataSource) instanceof JsonPrimitive){
                                JsonArray dataElements = new JsonArray();
                                dataElements.add(State.getInstance().getStateObject().get(dataSource));

                                List<JsonArray> distributedElements = distributeElements(dataElements, data.getConstraints(), children);

                                checkDistributedElements(distributedElements, data, values);

                            }else {
                                throw new NotImplementedException("Not implemented: " + dataValues.get(dataSource).getClass());
                            }
                        }
                    }
                } else {

                    /* Check if data should be passed */
                    if (data.getPassing() != null && data.getPassing()) {
                        passData(outValues, data, children, values);
                    }
                }
            }
        }
        return values;
    }

    /**
     * Pass the data to the next successor.
     *
     * @param outValues   output values.
     * @param data        input data specified in the workflow file.
     * @param numChildren number of children.
     * @param values      where the data should be added.
     */
    private void passData(Map<String, Object> outValues, DataIns data, int numChildren, ArrayList<Map<String, Object>> values) {

        /* Check if the output contains the specified key */
        if (outValues.containsKey(name + "/" + data.getName())) {

            /* Iterate over all children */
            for (int i = 0; i < numChildren; i++) {

                /* Check if there is data for the specified child */
                if (values.size() > i) {
                    values.get(i).put(data.getName(), outValues.get(name + "/" + data.getName()));
                } else {
                    Map<String, Object> tmp = new HashMap<>();
                    tmp.put(data.getName(), outValues.get(name + "/" + data.getName()));
                    values.add(i, tmp);
                }
            }
        } else {
            logger.error("Cannot Pass data {}. No such matching value could be found", data.getName());
        }
    }

    /**
     * Check for the distribution of data elements.
     *
     * @param distributedElements input list of distributed elements.
     * @param data                the dataIns specified in the workflow file.
     * @param values              result which are transferred.
     */
    private void checkDistributedElements(List<JsonArray> distributedElements, DataIns data, ArrayList<Map<String, Object>> values) {

        /* Iterate over all distributed elements */
        for (int i = 0; i < distributedElements.size(); i++) {

            Object block = distributedElements.get(i);
            if (distributedElements.get(i).size() == 1) {

                /* Extract a single value */
                JsonArray arr = distributedElements.get(i);
                block = "number".equals(data.getType()) ? arr.get(0).getAsNumber() : arr.get(0).getAsString();
            }

            // TODO check if this should be dynamic
            /* Define the key which should be used */
            String key = name + "/" + data.getName();

            /* Check if there are enough values */
            if (values.size() > i) {

                /* Use the child map we already created for another DataIns port */
                values.get(i).put(key, block);
            } else {

                /* Create a new map if there are not enough elements */
                Map<String, Object> map = new HashMap<>();
                map.put(key, block);
                // TODO check why i as key?
                values.add(i, map);
            }
        }
    }

    /**
     * Distributes the given elements in BLOCK mode. The collection is split into blocks of the given size.
     *
     * @param elements  The data elements to distribute.
     * @param blockSize The block size of each block.
     *
     * @return The data blocks in a list.
     */
    private List<JsonArray> distributeOutValsBlock(JsonArray elements, int blockSize) {
        List<JsonArray> blocks = new ArrayList<>();
        JsonArray currentBlock = new JsonArray();

        // check if the JsonArray contains the array as its first and only element
        String tmp = elements.get(0).toString().replaceAll("\"", "").replaceAll("\\\\", "");
        if (tmp.charAt(1) == '{' && tmp.charAt(tmp.length() - 2) == '}') {
            elements = JsonParser.parseString(elements.get(0).getAsString()).getAsJsonArray();
        } else {
            String[] items = elements.get(0).toString().split(",");
            if (elements.size() == 1 && items.length > 1) {
                elements = new Gson().toJsonTree(new Gson().fromJson(elements.get(0).getAsString(), String[].class)).getAsJsonArray();
            }
        }

        /* Iterate over the whole array and distribute the elements to the blocks */
        for (int i = 0; i < elements.size(); i++) {
            if (elements.get(i) instanceof JsonObject) {
                currentBlock.add(new Gson().toJson(elements.get(i)));
            } else {
                currentBlock.add(elements.get(i).getAsString().replace("[", "").replace("]", "").trim());
            }

            /* Complete the current block if it is full or we ran out of elements */
            if (currentBlock.size() >= blockSize || i == (elements.size() - 1)) {
                blocks.add(currentBlock);
                currentBlock = new JsonArray();
            }
        }

        return blocks;
    }

    /**
     * Distributes the given data elements across loop iterations taking into account the given constraints.
     *
     * @param dataElements the data elements to distribute
     * @param constraints  the constraints to consider
     * @param children     the number of children (iterations)
     *
     * @return a list containing the distributed elements
     */
    protected List<JsonArray> distributeElements(JsonArray dataElements, List<PropertyConstraint> constraints,
                                                 int children) {
        /* Check for unknown constraints */
        for (PropertyConstraint constraint : constraints) {
            if ("element-index".equals(constraint.getName()) || "distribution".equals(constraint.getName())) {
                continue;
            }
            throw new NotImplementedException("Constraint " + constraint.getName() + " not implemented.");
        }

        /* Element-index constraint has higher precedence than distribution constraint */
        PropertyConstraint elementIndexConstraint = getPropertyConstraintByName(constraints, "element-index");
        if (elementIndexConstraint != null) {

            /* Create a subset of the collection using the indices specified in the element-index constraint */
            List<Integer> indices = ElementIndex.parseIndices(elementIndexConstraint.getValue());
            JsonArray subset = new JsonArray(indices.size());
            for (Integer i : indices) {
                subset.add(dataElements.get(i));
            }
            dataElements = subset;
        }

        /* Check for the distribute constraint */
        List<JsonArray> distributedElements;
        PropertyConstraint distributionConstraint = getPropertyConstraintByName(constraints, "distribution");
        if (distributionConstraint != null) {

            /* Check for a block distribution */
            if (distributionConstraint.getValue().contains("BLOCK")) {

                /* Get the defined block size */
                int blockSize = Integer.parseInt(distributionConstraint.getValue().replaceAll("[^0-9?!.]", ""));

                /* Distribute the elements */
                distributedElements = distributeOutValsBlock(dataElements, blockSize);
            } else if (distributionConstraint.getValue().contains("REPLICATE")) {

                /* Determine the number of times the data should be replicated */
                int replicaSize;
                if (distributionConstraint.getValue().contains("REPLICATE(*)")) {
                    replicaSize = children;
                } else {
                    replicaSize = Integer.parseInt(distributionConstraint.getValue().replaceAll("[^0-9?!.]", ""));
                }

                /* Create an array of that specific size */
                distributedElements = new ArrayList<>();
                for (int i = 0; i < replicaSize; i++) {
                    distributedElements.add(dataElements);
                }
            } else {
                throw new NotImplementedException("Distribution type for " + distributionConstraint.getValue()
                        + " not implemented.");
            }
        } else {

            /* Provide the same elements to each child if no distribution constraint is specified */
            distributedElements = new ArrayList<>();
            for (int i = 0; i < children; i++) {
                distributedElements.add(dataElements);
            }
        }

        return distributedElements;
    }

    /**
     * Returns the first property constraint with the given name or {@code null} if it does not exist.
     *
     * @param propertyConstraints the property constraints
     * @param name                the name of the property constraint to be searched for
     *
     * @return the first property constraint with the given name or {@code null} if it does not exist
     */
    protected PropertyConstraint getPropertyConstraintByName(List<PropertyConstraint> propertyConstraints,
                                                             String name) {
        return propertyConstraints
                .stream()
                .filter(x -> x.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Getter and Setter
     */

    @Override
    public Map<String, Object> getResult() {
        return null;
    }

    public List<DataIns> getDataIns() {
        return dataIns;
    }

    public void setDataIns(List<DataIns> dataIns) {
        this.dataIns = dataIns;
    }
}
