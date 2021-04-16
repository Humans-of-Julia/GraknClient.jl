# This file is a part of GraknClient.  License is MIT: https://github.com/Humans-of-Julia/GraknClient.jl/blob/main/LICENSE

const PULSE_INTERVAL_MILLIS = 5000

mutable struct  CoreSession <: AbstractCoreSession
    client::CoreClient
    database::CoreDatabase
    sessionID::Bytes
    transactions::Dict{UUID,T} where {T<:Union{Nothing,<:AbstractCoreTransaction}}
    type::Int
    accessLock::ReentrantLock
    options::GraknOptions
    isOpen::Bool
    networkLatencyMillis::Int
    timer::Optional{Timer}
end

Base.show(io::IO, session::T) where {T<:AbstractCoreSession} = Base.print(io, session)
Base.print(io::IO, session::T) where {T<:AbstractCoreSession} = Base.print(io, "Session(ID: $(bytes2hex(session.sessionID)))")

function CoreSession(client::T, database::String , type::Int32 , options::GraknOptions = GraknOptions()) where {T<:AbstractCoreClient}
    try
        options.session_idle_timeout_millis = PULSE_INTERVAL_MILLIS
        #building open_request
        open_req = SessionRequestBuilder.open_req(
            database, type , copy_to_proto(options, grakn.protocol.Options)
        )
        # open the session
        startTime = now()
        req_result, status  = session_open(client.core_stub.blockingStub, gRPCController(), open_req)
        res_id = grpc_result_or_error(req_result, status, result->result.session_id)
        endTime = now()

        database = CoreDatabase(database)
        networkLatencyMillis = (endTime - startTime).value
        session_id =  res_id
        transactions = Dict{UUID,AbstractCoreTransaction}()
        is_open = true

        result = CoreSession(client, database, session_id, transactions, type, ReentrantLock() ,options, is_open, networkLatencyMillis, nothing)

        # prepare the pulse_request function with a timer
        cb(timer) = (make_pulse_request(result))
        # don't touch the delay formula except you know what you are doing
        # the delay is crucial for session keep alive
        delay = (PULSE_INTERVAL_MILLIS / 1000) - 3
        t = Timer(cb,delay - 1, interval= delay)

        # keep the timer in the transaction to close the timer later
        result.timer = t

        return result
    catch ex
        throw(GraknClientException("Error construct a CoreSession",ex))
    end
end

"""
function make_pulse_request(session::T) where {T<:AbstractCoreSession}
    This function make a pulse request to keep the session alive.
"""
function make_pulse_request(session::T) where {T<:AbstractCoreSession}
    try
        pulsreq = SessionRequestBuilder.pulse_req(session.sessionID)
        req_result, status = session_pulse(session.client.core_stub.blockingStub, gRPCController() , pulsreq)
        result = grpc_result_or_error(req_result,status, result->result)

        if result.alive === false
            session.isOpen = false
            close(session.timer)
            @info "$session is closed"
        end
    catch ex
        @info "make_pulse_request show's an error"
    finally
    end
end

function transaction(session::T, type::Int32) where {T<:AbstractCoreSession}
        return transaction(session, type, grakn_options_core())
end

function transaction(session::T, type::Int32, options::GraknOptions) where {T<:AbstractCoreSession}
    try
        lock(session.accessLock)
        if !session.isOpen
            throw(GraknClientException(CLIENT_SESSION_CLOSED, bytes2hex(session.sessionID)))
        end

        transactionRPC = CoreTransaction(session, session.sessionID, type, options)
        session.transactions[transactionRPC.transaction_id] = transactionRPC

        return transactionRPC
    finally
       unlock(session.accessLock)
    end
end

function close(session::T) where {T<:AbstractCoreSession}
    try
        lock(session.accessLock)
        if session.isOpen
            for (uuid,trans) in session.transactions
                close(trans)
                delete!(session.transactions, trans.transaction_id)
            end
            remove_session(session.client, session)
            close(session.timer)

            req = SessionRequestBuilder.close_req(session.sessionID)
            stub = session.client.core_stub.blockingStub
            session_close(stub, gRPCController(), req )

            session.isOpen = false
        end
    catch  ex
        throw(GraknClientException("Unexpected error while closing session ID: $(session.sessionID)",ex))
        @info ex
    finally
        unlock(session.accessLock)
    end
end



#
# package grakn.client.core;
#
# import com.google.protobuf.ByteString;
# import grakn.client.api.GraknOptions;
# import grakn.client.api.GraknSession;
# import grakn.client.api.GraknTransaction;
# import grakn.client.common.exception.GraknClientException;
# import grakn.client.common.rpc.GraknStub;
# import grakn.client.stream.RequestTransmitter;
# import grakn.common.collection.ConcurrentSet;
# import grakn.protocol.SessionProto;
# import io.grpc.StatusRuntimeException;
#
# import java.time.Duration;
# import java.time.Instant;
# import java.util.Timer;
# import java.util.TimerTask;
# import java.util.concurrent.atomic.AtomicBoolean;
# import java.util.concurrent.locks.ReadWriteLock;
# import java.util.concurrent.locks.StampedLock;
#
# import static grakn.client.common.exception.ErrorMessage.Client.SESSION_CLOSED;
# import static grakn.client.common.rpc.RequestBuilder.Session.closeReq;
# import static grakn.client.common.rpc.RequestBuilder.Session.openReq;
# import static grakn.client.common.rpc.RequestBuilder.Session.pulseReq;
#
# public class CoreSession implements GraknSession {
#
#     private static final int PULSE_INTERVAL_MILLIS = 5_000;
#
#     private final CoreClient client;
#     private final CoreDatabase database;
#     private final ByteString sessionID;
#     private final ConcurrentSet<GraknTransaction.Extended> transactions;
#     private final Type type;
#     private final GraknOptions options;
#     private final Timer pulse;
#     private final ReadWriteLock accessLock;
#     private final AtomicBoolean isOpen;
#     private final int networkLatencyMillis;
#
#     public CoreSession(CoreClient client, String database, Type type, GraknOptions options) {
#         try {
#             this.client = client;
#             this.type = type;
#             this.options = options;
#             Instant startTime = Instant.now();
#             SessionProto.Session.Open.Res res = client.stub().sessionOpen(
#                     openReq(database, type.proto(), options.proto())
#             );
#             Instant endTime = Instant.now();
#             this.database = new CoreDatabase(client.databases(), database);
#             networkLatencyMillis = (int) (Duration.between(startTime, endTime).toMillis() - res.getServerDurationMillis());
#             sessionID = res.getSessionId();
#             transactions = new ConcurrentSet<>();
#             accessLock = new StampedLock().asReadWriteLock();
#             isOpen = new AtomicBoolean(true);
#             pulse = new Timer();
#             pulse.scheduleAtFixedRate(this.new PulseTask(), 0, PULSE_INTERVAL_MILLIS);
#         } catch (StatusRuntimeException e) {
#             throw GraknClientException.of(e);
#         }
#     }
#
#     @Override
#     public boolean isOpen() { return isOpen.get(); }
#
#     @Override
#     public Type type() { return type; }
#
#     @Override
#     public CoreDatabase database() { return database; }
#
#     @Override
#     public GraknOptions options() { return options; }
#
#     @Override
#     public GraknTransaction transaction(GraknTransaction.Type type) {
#         return transaction(type, GraknOptions.core());
#     }
#
#     @Override
#     public GraknTransaction transaction(GraknTransaction.Type type, GraknOptions options) {
#         try {
#             accessLock.readLock().lock();
#             if (!isOpen.get()) throw new GraknClientException(SESSION_CLOSED);
#             GraknTransaction.Extended transactionRPC = new CoreTransaction(this, sessionID, type, options);
#             transactions.add(transactionRPC);
#             return transactionRPC;
#         } finally {
#             accessLock.readLock().unlock();
#         }
#     }
#
#     ByteString id() { return sessionID; }
#
#     GraknStub.Core stub() {
#         return client.stub();
#     }
#
#     RequestTransmitter transmitter() {
#         return client.transmitter();
#     }
#
#     int networkLatencyMillis() { return networkLatencyMillis; }
#
#     @Override
#     public void close() {
#         try {
#             accessLock.writeLock().lock();
#             if (isOpen.compareAndSet(true, false)) {
#                 transactions.forEach(GraknTransaction.Extended::close);
#                 client.removeSession(this);
#                 pulse.cancel();
#                 try {
#                     SessionProto.Session.Close.Res ignore = stub().sessionClose(closeReq(sessionID));
#                 } catch (StatusRuntimeException e) {
#                     // Most likely the session is already closed or the server is no longer running.
#                 }
#             }
#         } catch (StatusRuntimeException e) {
#             throw GraknClientException.of(e);
#         } finally {
#             accessLock.writeLock().unlock();
#         }
#     }
#
#     private class PulseTask extends TimerTask {
#
#         @Override
#         public void run() {
#             if (!isOpen()) return;
#             boolean alive;
#             try {
#                 alive = stub().sessionPulse(pulseReq(sessionID)).getAlive();
#             } catch (StatusRuntimeException exception) {
#                 alive = false;
#             }
#             if (!alive) {
#                 isOpen.set(false);
#                 pulse.cancel();
#             }
#         }
#     }
# }
