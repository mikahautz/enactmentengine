package at.enactmentengine.serverless.nodes;

import at.enactmentengine.serverless.exception.MissingInputDataException;
import at.enactmentengine.serverless.object.State;
import at.uibk.dps.afcl.functions.objects.DataIns;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
 * Control node which manages the tasks at the start of a parallel loop.
 *
 * @author markusmoosbrugger, jakobnoeckl
 * adapted by @author stefanpedratscher
 */
public class ParallelStartNode extends Node {

    /**
     * Logger for the parallel-start node.
     */
    static final Logger logger = LoggerFactory.getLogger(ParallelStartNode.class);
    /**
     * The maximum number of threads running in parallel
     */
    private static final int MAX_NUMBER_THREADS = 1000;

    /**
     * Specifies if its parent is a parallelFor and counts how many SimulationNodes are children of this node.
     */
    public long isAfterParallelForNode = -1;
    /**
     * The input defined within the workflow file.
     */
    private List<DataIns> definedInput;

    /**
     * Default constructor for the parallel-start node.
     *
     * @param name         of the parallel-start node.
     * @param type         of the parallel-start node.
     * @param definedInput input defined in the workflow file.
     */
    public ParallelStartNode(String name, String type, List<DataIns> definedInput) {
        super(name, type);
        this.definedInput = definedInput;
    }

    /**
     * Checks the dataValues and creates a thread pool for the execution of the
     * children.
     */
    @Override
    public Boolean call() throws Exception {

        JsonObject state = State.getInstance().getStateObject();

        /* Check if there is an input defined */
        if (definedInput != null) {

            /* Iterate over the possible inputs and look for defined ones */
            for (DataIns data : definedInput) {
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

                boolean gotDataFromDataSource = false;

                for (String dataSource : sourceList) {

                    String subObject = null;

                    long count = data.getSource().chars().filter(ch -> ch == '/').count();
                    if (count > 1) {
                        subObject = State.getInstance().findJSONSubObject(data.getSource().substring(0, data.getSource().indexOf("/", data.getSource().indexOf("/") + 1)), data.getSource().substring(data.getSource().indexOf("/", data.getSource().indexOf("/") + 1) + 1), count);
                    }

                    if(state.get(dataSource) != null || subObject != null) {
                        String toUse = subObject != null ? subObject : State.getInstance().getStateObject().get(dataSource).toString();

                        state.add(name + "/" + data.getName() + (this.getId() != 0 ? "/" + this.getId() : ""), new Gson().fromJson(toUse, JsonElement.class));
                        gotDataFromDataSource = true;
                    }
                }

                if(!gotDataFromDataSource){
                    throw new MissingInputDataException(ParallelStartNode.class.getCanonicalName() + ": " + name
                            + " needs " + data.getSource() + "!");
                }
            }
        }

        logger.info("Executing {} ParallelStartNodeOld", name);

        /* Create an executor service to manage parallel running threads */
        ExecutorService exec = Executors
                .newFixedThreadPool(children.size() > MAX_NUMBER_THREADS ? MAX_NUMBER_THREADS : children.size());

        long simNodes = 0;
        /* Pass data to all children and execute them */
        List<Future<Boolean>> futures = new ArrayList<>();
        for (Node node : children) {
            if (node instanceof SimulationNode) {
                simNodes++;
            }
            if (getLoopCounter() != -1) {
                node.setLoopCounter(loopCounter);
                node.setMaxLoopCounter(maxLoopCounter);
                node.setConcurrencyLimit(concurrencyLimit);
                node.setStartTime(startTime);
            }
            futures.add(exec.submit(node));
        }

        // specify how many functions are directly after a nested construct (needed if concurrency limit is exceeded)
        if (isAfterParallelForNode != -1) {
            for (Node node : children) {
                if (node instanceof IfStartNode) {
                    ((IfStartNode) node).isAfterParallelForNode = isAfterParallelForNode + simNodes;
                } else if (node instanceof ParallelStartNode) {
                    ((ParallelStartNode) node).isAfterParallelForNode = isAfterParallelForNode + simNodes;
                } else if (node instanceof SwitchStartNode) {
                    ((SwitchStartNode) node).isAfterParallelForNode = isAfterParallelForNode + simNodes;
                } else if (node instanceof SimulationNode) {
                    ((SimulationNode) node).setAmountParallelFunctions(isAfterParallelForNode + simNodes);
                }
            }
        }

        /* Wait for all children to finish */
        for (Future<Boolean> future : futures) {
            future.get();
        }
        exec.shutdown();

        return true;
    }

    /**
     *  Saves the passed result as dataValues.
     *
     * @param input to be passed
     */
    @Override
    public void passResult(Map<String, Object> input) {
        synchronized (this) {

            /* Check if the map containing the actual values is already defined */
            if (dataValues == null) {
                dataValues = new HashMap<>();
            }

            /* Check if there is an input defined */
            if (definedInput != null) {

                /* Iterate over the defined input and look for a match with the actual value */
                for (DataIns data : definedInput) {
                    if (input.containsKey(data.getSource())) {
                        dataValues.put(data.getSource(), input.get(data.getSource()));
                    }
                }
            }
        }
    }

    /**
     * Return the result of the parallel-start node.
     *
     * @return null because a start node does not return anything.
     */
    @Override
    public Map<String, Object> getResult() {
        return null;
    }

    /**
     * Clones this node and its children.
     *
     * @param endNode end node.
     *
     * @return cloned node.
     * @throws CloneNotSupportedException on failure.
     */
    @Override
    public Node clone(Node endNode) throws CloneNotSupportedException {
        Node node = (Node) super.clone();

        /* Find children nodes */
        node.children = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            Node currNode = children.get(i).clone(endNode);
            node.children.add(currNode);
            if (i == 0) {
                endNode = findParallelEndNode(currNode, 0);
            }
        }

        return node;
    }

    /**
     * Finds the matching parallel-end node recursively.
     *
     * @param currentNode the start node to search for.
     * @param depth of the parallel nesting
     *
     * @return found parallel-end node.
     */
    private Node findParallelEndNode(Node currentNode, int depth) {

        /* Iterate over all children */
        for (Node child : currentNode.getChildren()) {

            /* Check if end node is found */
            if (child instanceof ParallelEndNode) {

                /* Check if dept is correct */
                if (depth == 0) {
                    return child;
                } else {
                    return findParallelEndNode(child, depth - 1);
                }
            } else if (child instanceof ParallelStartNode) {

                /* Another nested parallel node detected */
                return findParallelEndNode(child, depth + 1);
            } else {

                /* Another (compound or base) function detected */
                return findParallelEndNode(child, depth);
            }
        }
        return null;
    }
}
