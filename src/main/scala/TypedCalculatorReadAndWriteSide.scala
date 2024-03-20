import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, Graph, SinkShape}
import akka_typed.CalculatorRepository.{createSession, updateresultAndOffset}
import akka_typed.TypedCalculatorWriteSide._
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}
import slick.jdbc.GetResult

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.language.postfixOps

object akka_typed {
  trait CborSerialization
  val persId = PersistenceId.ofUniqueId("001")

  implicit val session: SlickSession = createSession()

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Double) extends Event
    case class Multiplied(id: Int, amount: Double) extends Event
    case class Divided(id: Int, amount: Double) extends Event

    final case class State(value: Double) extends CborSerialization{
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    object State{
      val empty=State(0)
    }

    def handleCommand(
                       persId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding for number: $amount and state is ${state.value} ")
          val added = Added(persId.toInt, amount)
          Effect.persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiply for number: $amount and state is ${state.value} ")
          val multiplied = Multiplied(persId.toInt, amount)
          Effect.persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive divide for number: $amount and state is ${state.value} ")
          val divided = Divided(persId.toInt, amount)
          Effect.persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling Event added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling Event multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling Event divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]){
    import CalculatorRepository.Result
    implicit val materializer = system.classicSystem

    val res: Result = CalculatorRepository.getlatestOffsetAndResult match {
      case Right(value) => value
      case Left(_) =>
        println("Error while getting latest offset and result, use default value")
        Result(0, 1)
    }
    var latestCalculatedResult = res.state
    var offset = res.offset

    val startOffset: Long = if (offset == 1) 1 else offset + 1
    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    /*
    В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala) необходимо разделить чтение событий, бизнес логику и запись в целевой получатель и сделать их асинхронными, т.е.
    1) Persistence Query должно находиться в Source
    2) Обновление состояния необходимо переместить в отдельный от записи в БД flow, замкнуть пуcтым sink

    Как делать!!!
    1. в типах int заменить на double
    2. добавить функцию updateState, в которой сделать паттерн матчинг для событий Added, Multiplied, Divided
    3. нужно создать graphDsl в котором builder.add(source)
    4. builder.add(Flow[EventEnvelope].map(e=> updateState(e.event, e.sequenceNr)))

     */

    /*
    spoiler
    def updateState(event: Any, seqNum:Long) : Result = {
    val newState = event match {
    case Added(_, amount) =>???
    case Multiplied(_, amount) =>???
    case Divided(_, amount) =>???

    val graph = GraphDSL.Builder[NotUsed] =>
    //1.
    val input = builder.add(source)
    val stateUpdate = builder.add(Flow[EventEnvelope].map(e=> updateState(...)))
    val localSaveOutput = builder.add(Sink.foreach[Result]{
    r=>
    lCr = r.state
    //logs

    })

    val dbSaveOutput = builder.add(
    Sink.sink[Result](r=>updateresultAndOffset)
    )
    надо разделить builder на 2 части
    далее надо сохранить flow(уже разделен на 2 части) в 1. localSaveOutput 2. dbSaveOutput
    закрываем граф и запускаем

     */

    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    val program: Graph[ClosedShape, NotUsed] = GraphDSL.create() { builder =>

      val input = builder.add(source)
      val stateUpdate = builder.add(Flow[EventEnvelope].map(e => updateState(e)))
      val localSaveOutput: SinkShape[Result] = builder.add(Sink.foreach[Result](r => {
        println(r)
        latestCalculatedResult + r.state
      }))
      val dbSaveOutput: SinkShape[Result] = builder.add(Sink.foreach[Result](r => updateresultAndOffset(r.state, r.offset)))

      val broadcasting = builder.add(Broadcast[Result](2))
      input -> stateUpdate -> broadcasting

      broadcasting.out(0) -> localSaveOutput
      broadcasting.out(1) -> dbSaveOutput

      ClosedShape
    }

    def updateState(eventEnvelope: EventEnvelope): CalculatorRepository.Result = {
      Result (
        eventEnvelope.event match {
          case Added(_, amount) =>
            latestCalculatedResult + amount
          case Multiplied(_, amount) =>
            latestCalculatedResult * amount
          case Divided(_, amount) =>
            latestCalculatedResult / amount
        },
        eventEnvelope.sequenceNr)
    }
  }

  object CalculatorRepository {
    //homework, how to do
    //1. SlickSession здесь надо посмотреть документацию
    def createSession(): SlickSession = {
      SlickSession.forConfig("slick")
    }

    def initdatabase: Unit = {
      Class.forName("org.postgresql.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100, driverName = "org.postgresql.Driver")
      ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
    }

    // homework
    case class Result(state: Double, offset: Long)
    // надо переделать getlatestOffsetAndResult
    // def getlatestOffsetAndResult: Result = {
    // val query = sql"select * from public.result where id = 1;".as[Double].headOption
    // надо будет создать future для db.run
    // с помощью await надо получить результат или прокинуть ошибку если результата нет

    def getlatestOffsetAndResult (implicit session: SlickSession): Either[Exception, Result] = {
      import session.profile.api._
      implicit val getUserResult: GetResult[Result] = GetResult(row => Result(row.nextDouble(), row.nextLong()))

      val query: Future[Option[Result]] = session.db.run(
        sql"select calculated_value, write_side_offset from public.result where id = 1"
        .as[Result].headOption)

      Await.result( query, 5 second).toRight(new Exception("No result"))
    }

    def updateresultAndOffset(calculated: Double, offset: Long)(implicit session: SlickSession): Unit = {
      import session.profile.api._
      Slick.sink[Result] { r: Result
        => sqlu"update public.result set calculated_value = ${calculated}, write_side_offset = ${offset} where id = 1"
      }
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeActorRef ! Add(10)
        writeActorRef ! Multiply(2)
        writeActorRef ! Divide(5)
        Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    implicit  val executionContext: ExecutionContextExecutor = system.executionContext

    val program = TypedCalculatorReadSide(system).program
    RunnableGraph.fromGraph(program).run()
  }


}