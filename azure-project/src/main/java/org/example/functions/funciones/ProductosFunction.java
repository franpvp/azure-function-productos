package org.example.functions.funciones;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.EventGridPublisherClient;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import org.example.functions.dto.ProductoDto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ProductosFunction {

    static final ObjectMapper mapper = new ObjectMapper()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @FunctionName("obtenerProductos")
    public HttpResponseMessage obtenerProductos(
            @HttpTrigger(
                    name = "reqGetAll",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "productos"
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        List<ProductoDto> productos = new ArrayList<>();

        try {

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv("ORACLE_WALLET_DIR");
            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }

            // 2) Conectar usando TNS_ADMIN hacia tu alias _tp del tnsnames.ora
            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT ID, NOMBRE, DESCRIPCION, PRECIO FROM PRODUCTO");
                 ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    productos.add(ProductoDto.builder()
                            .id(rs.getLong("ID"))
                            .nombre(rs.getString("NOMBRE"))
                            .descripcion(rs.getString("DESCRIPCION"))
                            .precio(rs.getInt("PRECIO"))
                            .build());
                }
            }
        } catch (Exception e) {
            context.getLogger().severe("Error consultando Oracle: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al obtener productos: " + e.getMessage())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(productos) // Jackson lo serializa
                .build();
    }

    @FunctionName("getProductoById")
    public HttpResponseMessage getProductoById(
            @HttpTrigger(
                    name = "reqGetById",
                    methods = {HttpMethod.GET},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        ProductoDto producto = null;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM PRODUCTO WHERE ID = ?")) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                producto = ProductoDto.builder()
                        .id(rs.getLong("ID"))
                        .nombre(rs.getString("NOMBRE"))
                        .descripcion(rs.getString("DESCRIPCION"))
                        .precio(rs.getInt("PRECIO"))
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Error al obtener producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error: " + e.getMessage())
                    .build();
        }

        if (producto == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body("Producto no encontrado con ID " + id)
                    .build();
        }
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(producto)
                .build();
    }


    @FunctionName("crearProducto")
    public HttpResponseMessage crearProducto(
            @HttpTrigger(
                    name = "reqPost",
                    methods = {HttpMethod.POST},
                    route = "productos",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        // final String walletEnv              = System.getenv("ORACLE_WALLET_DIR");
        final String oracleUser             = System.getenv("ORACLE_USER");
        final String oraclePass             = System.getenv("ORACLE_PASS");
        final String oracleTnsName          = System.getenv("ORACLE_TNS_NAME");

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        try {
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body("El body no puede ser vacío")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            ProductoDto nuevo = mapper.readValue(body, ProductoDto.class);

            String walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
            String walletEnv = System.getenv("ORACLE_WALLET_DIR");
            if (walletEnv != null && !walletEnv.isBlank()) {
                walletPath = walletEnv;
            }
            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO PRODUCTO (NOMBRE, DESCRIPCION, PRECIO) VALUES (?, ?, ?)")) {

                stmt.setString(1, nuevo.getNombre());
                stmt.setString(2, nuevo.getDescripcion());
                stmt.setInt(3, nuevo.getPrecio());
                stmt.executeUpdate();
            }

            EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                    .endpoint(eventGridTopicEndpoint)
                    .credential(new AzureKeyCredential(eventGridTopicKey))
                    .buildEventGridEventPublisherClient();

            // Payload del evento
            ProductoDto eventData = ProductoDto.builder()
                    .nombre(nuevo.getNombre())
                    .descripcion(nuevo.getDescripcion())
                    .precio(nuevo.getPrecio())
                    .build();

            EventGridEvent event = new EventGridEvent(
                    "/api/productos",
                    "api.producto.creado.v1",
                    BinaryData.fromObject(eventData),
                    "0.1"
            );
            client.sendEvent(event);

            return request.createResponseBuilder(HttpStatus.OK)
                    .body("Evento creado correctamente")
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error creando producto: " + e.getMessage());
            return json(request, HttpStatus.INTERNAL_SERVER_ERROR, Map.of(
                    "error", "Error al crear producto",
                    "detalle", e.getMessage()
            ));
        }
    }

    @FunctionName("modificarProducto")
    public HttpResponseMessage updateProducto(
            @HttpTrigger(
                    name = "reqPut",
                    methods = {HttpMethod.PUT},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<ProductoDto>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

//        final String eventGridTopicEndpoint = System.getenv("EVENTGRID_TOPIC_ENDPOINT");
//        final String eventGridTopicKey      = System.getenv("EVENTGRID_TOPIC_KEY");

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        ProductoDto actualizado = request.getBody().orElse(null);
        if (actualizado == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Debe enviar un producto en el body")
                    .build();
        }

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE PRODUCTO SET NOMBRE=?, DESCRIPCION=?, PRECIO=? WHERE ID=?")) {
            stmt.setString(1, actualizado.getNombre());
            stmt.setString(2, actualizado.getDescripcion());
            stmt.setInt(3, actualizado.getPrecio());
            stmt.setLong(4, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al actualizar producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }
        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe producto con ID " + id).build();
        }

        try {
            EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                    .endpoint(eventGridTopicEndpoint)
                    .credential(new AzureKeyCredential(eventGridTopicKey))
                    .buildEventGridEventPublisherClient();

            ProductoDto eventData = ProductoDto.builder()
                    .id(id)
                    .nombre(actualizado.getNombre())
                    .descripcion(actualizado.getDescripcion())
                    .precio(actualizado.getPrecio())
                    .build();

            EventGridEvent event = new EventGridEvent(
                    "/api/productos",
                    "api.producto.actualizado.v1",
                    BinaryData.fromObject(eventData),
                    "1.0"
            );

            event.setEventTime(OffsetDateTime.now());
            client.sendEvent(event);

        } catch (Exception e) {
            context.getLogger().severe("Producto actualizado pero falló publicar evento: " + e.getMessage());
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("Producto actualizado con éxito")
                .build();
    }

    @FunctionName("eliminarProducto")
    public HttpResponseMessage deleteProducto(
            @HttpTrigger(
                    name = "reqDelete",
                    methods = {HttpMethod.DELETE},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM PRODUCTO WHERE ID=?")) {
            stmt.setLong(1, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe producto con ID " + id).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Producto eliminado con éxito").build();
    }

    private HttpResponseMessage json(HttpRequestMessage<?> req, HttpStatus status, Object body) {
        return req.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(asJson(body))
                .build();
    }

    private String asJson(Object o) {
        try { return mapper.writeValueAsString(o); }
        catch (Exception e) { return "{\"error\":\"No se pudo serializar JSON\"}"; }
    }

}
