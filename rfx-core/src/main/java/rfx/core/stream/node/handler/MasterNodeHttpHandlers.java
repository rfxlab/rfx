package rfx.core.stream.node.handler;

import java.util.Date;

import org.apache.commons.net.util.Base64;

import com.google.gson.Gson;

import io.vertx.core.MultiMap;
import rfx.core.model.HttpServerRequestCallback;
import rfx.core.stream.cluster.ClusterDataManager;
import rfx.core.util.FileUtils;

/**
 * the HTTP Request Handlers for Master Node
 * 
 * <br>
 * 
 * @author trieu
 *
 */
public class MasterNodeHttpHandlers extends HttpHandlerMapper {

    @Override
    public HttpHandlerMapper buildCallbackHandlers() {
        when("/server-time", new HttpServerRequestCallback() {
            @Override
            protected String onHttpGetOk() {
                return new Date().toString();
            }
        });

        when("/json/deploy-topology", new HttpServerRequestCallback() {
            @Override
            protected String onHttpPostOk(MultiMap formData) {
                String base64data = formData.get("base64data");
                String token = formData.get("token");
                String filename = formData.get("filename");
                System.out.println("token " + token);
                deployJarFileFromBase64Data(base64data, filename);
                return "ok:" + token;
            }
        });
        when("/json/get-workers", new HttpServerRequestCallback() {
            @Override
            protected String onHttpGetOk() {
                return new Gson().toJson(ClusterDataManager.getWorkerData());
            }
        });
        return this;
    }

    public static boolean deployJarFileFromBase64Data(final String base64Data, final String filepath) {
        byte[] decoded = Base64.decodeBase64(base64Data.getBytes());
        FileUtils.writeStringToFile(filepath, decoded);
        return true;
    }

}
