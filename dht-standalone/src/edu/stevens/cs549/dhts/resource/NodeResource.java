package edu.stevens.cs549.dhts.resource;

import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/dht")
public class NodeResource {

	/*
	 * Web service API.
	 * 
	 * TODO: Fill in the missing operations.
	 */

	Logger log = Logger.getLogger(NodeResource.class.getCanonicalName());

	@Context
	UriInfo uriInfo;

	@Context
	HttpHeaders headers;

	@GET
	@Path("info")
	@Produces("application/xml")
	public Response getNodeInfoXML() {
		return new NodeService(headers, uriInfo).getNodeInfo();
	}

	@GET
	@Path("info")
	@Produces("application/json")
	public Response getNodeInfoJSON() {
		return new NodeService(headers, uriInfo).getNodeInfo();
	}

	@GET
	@Path("pred")
	@Produces("application/xml")
	public Response getPred() {
		return new NodeService(headers, uriInfo).getPred();
	}
	
	@GET
	@Path("succ")
	@Produces("application/xml")
	public Response getSucc() {
		return new NodeService(headers, uriInfo).getSucc();
	}
	
	@PUT
	@Path("notify")
	@Consumes("application/xml")
	@Produces("application/xml")
	/*
	 * Actually returns a TableRep (annotated with @XmlRootElement)
	 */
	public Response putNotify(TableRep predDb) {
		/*
		 * See the comment for WebClient::notify (the client side of this logic).
		 */
		return new NodeService(headers, uriInfo).notify(predDb);
		// NodeInfo p = predDb.getInfo();
	}

	@GET
	@Path("find")	
	@Produces("application/xml")
	public Response findSuccessor(@QueryParam("id") String index) {
		int id = Integer.parseInt(index);
		return new NodeService(headers, uriInfo).findSuccessor(id);
	}
	
	@PUT
	@Path("setKeyValue")
	@Consumes("application/xml")
	public Response setKeyValue(TableRep rep) {
		return new NodeService(headers, uriInfo).setKeyValue(rep.entry[0].key, rep.entry[0].vals[0]);
	}
	
	@GET
	@Path("getValue")
	@Produces("application/xml")
	public Response getKeyValue(@QueryParam("key") String key) {
		return new NodeService(headers, uriInfo).getKeyValue(key);
	}
	
	@DELETE
	@Path("delete")
	@Consumes("application/xml")
	public Response deleteKeyValue(@QueryParam("key") String key, @QueryParam("value") String value) {
		return new NodeService(headers, uriInfo).deleteKeyValue(key, value);
	}
	
	@GET
	@Path("finger")
	@Produces("application/xml")
	public Response closestPrecedingFinger(@QueryParam("id") int id) {
		return new NodeService(headers, uriInfo).closestPrecedingFinger(id);
	}

}
