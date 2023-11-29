package fs2

import cats.arrow.Strong
import cats.{Contravariant, Functor, Monad}
import cats.data.AndThen
import cats.syntax.all._

final class EvalScan[F[_], S, I, O](
    val initial: S,
    private val transform_ : AndThen[(S, I), F[(S, Chunk[O])]],
    private val onComplete_ : AndThen[S, F[Chunk[O]]]
)(implicit ev: Monad[F]) {

  /** Transformation function. */
  def transform(s: S, i: I): F[(S, Chunk[O])] = transform_((s, i))

  /** Chunk form of [[transform]]. */
  def transformAccumulate(s: S, c: Chunk[I]): F[(S, Chunk[O])] =
    c.foldLeft((s, Chunk.empty[O]).pure[F]) { case (effect, next) =>
      effect.flatMap { case (s, acc) =>
        transform(s, next).map { case (s2, os) =>
          (s2, acc ++ os)
        }
      }
    }

  /** Completion function. */
  def onComplete(s: S): F[Chunk[O]] = onComplete_(s)

  /** Converts this scan to a pipe. */
  def toPipe: Stream[F, I] => Stream[F, O] = {
    _.evalMapAccumulate(initial)(transform)
      .map(_._2)
      .unchunks
  }

  /** Steps this scan by a single input, returning a new scan and the output elements computed from the input. */
  def step(i: I): F[(EvalScan[F, S, I, O], Chunk[O])] = {
    transform(initial, i).map { case (s, os) =>
      (new EvalScan(s, transform_, onComplete_), os)
    }
  }

  /** Composes the supplied scan with this scan.
    *
    * The resulting scan maintains the state of each of the input scans independently.
    */
  def andThen[S2, O2](that: EvalScan[F, S2, O, O2]): EvalScan[F, (S, S2), I, O2] =
    EvalScan[F, (S, S2), I, O2]((initial, that.initial))(
      { case ((s, s2), i) =>
        transform(s, i).flatMap { case (sp, os) =>
          that.transformAccumulate(s2, os).map { case (s2p, out) =>
            ((sp, s2p), out)
          }
        }
      },
      { case (s, s2) =>
        onComplete(s).flatMap(that.transformAccumulate(s2, _)).flatMap { case (s3, out) =>
          that.onComplete(s3).map(out ++ _)
        }
      }
    )

  /** Returns a new scan which transforms output values using the supplied function. */
  def map[O2](f: O => O2): EvalScan[F, S, I, O2] =
    new EvalScan(
      initial,
      transform_.andThen(_.map { case (s, os) =>
        (s, os.map(f))
      }),
      onComplete_.andThen(_.map(_.map(f)))
    )

  /** Returns a new scan which transforms input values using the supplied function. */
  def contramap[I2](f: I2 => I): EvalScan[F, S, I2, O] =
    new EvalScan(
      initial,
      AndThen[(S, I2), (S, I)] { case (s, i2) => (s, f(i2)) }.andThen(transform_),
      onComplete_
    )

  def dimap[I2, O2](g: I2 => I)(f: O => O2): EvalScan[F, S, I2, O2] =
    EvalScan[F, S, I2, O2](initial)(
      { (s, i2) =>
        transform(s, g(i2)).map { case (s2, os) =>
          (s2, os.map(f))
        }
      },
      onComplete_.andThen(_.map(_.map(f)))
    )

  /** Transforms the state type. */
  def imapState[S2](g: S => S2)(f: S2 => S): EvalScan[F, S2, I, O] =
    EvalScan[F, S2, I, O](g(initial))(
      { (s2, i) =>
        transform(f(s2), i).map { case (s3, os) =>
          (g(s3), os)
        }
      },
      AndThen(f).andThen(onComplete_)
    )

  /** Returns a new scan with transformed input and output types.
    *
    * Upon receiving an `I2`, `get` is invoked and the result is fed to the
    * original scan. For each output value, `set` is invoked with the original
    * `I2` input and the computed `O`, yielding a new output of type `O2`.
    */
  def lens[I2, O2](get: I2 => I, set: (I2, O) => O2): EvalScan[F, S, I2, O2] =
    EvalScan[F, S, I2, O2](initial)(
      { (s, i2) =>
        transform(s, get(i2)).map { case (s2, os) =>
          (s2, os.map(s => set(i2, s)))
        }
      },
      _ => Chunk.empty[O2].pure[F]
    )

  /** Returns a scan that inputs/outputs pairs of elements, with `I` and `O` in the first element of the pair. */
  def first[A]: EvalScan[F, S, (I, A), (O, A)] =
    lens(_._1, (t, o) => (o, t._2))

  /** Returns a scan that inputs/outputs pairs of elements, with `I` and `O` in the second element of the pair. */
  def second[A]: EvalScan[F, S, (A, I), (A, O)] =
    lens(_._2, (t, o) => (t._1, o))

  /** Like [[lens]] but some elements are passed to the output (skipping the original scan) while other elements
    * are lensed through the original scan.
    */
  def semilens[I2, O2](extract: I2 => Either[O2, I], inject: (I2, O) => O2): EvalScan[F, S, I2, O2] =
    EvalScan[F, S, I2, O2](initial)(
      (s, i2) =>
        extract(i2).fold(
          o2 => (s, Chunk.singleton(o2)).pure[F],
          i => {
            transform(s, i).map { case (s2, os) =>
              (s2, os.map(s => inject(i2, s)))
            }
          }
        ),
      _ => Chunk.empty[O2].pure[F]
    )

  /** Like [[semilens]] but the elements of the original scan are output directly. */
  def semipass[I2, O2 >: O](extract: I2 => Either[O2, I]): EvalScan[F, S, I2, O2] =
    semilens(extract, (_, o) => o)

  /** Returns a scan that wraps the inputs/outputs with `Either`.
    * Elements on the left pass through the original scan while elements on
    * the right pass through directly.
    */
  def left[A]: EvalScan[F, S, Either[I, A], Either[O, A]] =
    semilens(_.fold(i => Right(i), a => Left(Right(a))), (_, o) => Left(o))

  /** Returns a scan that wraps the inputs/outputs with `Either`.
    * Elements on the right pass through the original scan while elements on
    * the left pass through directly.
    */
  def right[A]: EvalScan[F, S, Either[A, I], Either[A, O]] =
    semilens(_.fold(a => Left(Left(a)), i => Right(i)), (_, o) => Right(o))

  /** Combines this scan with the supplied scan such that elements on the left
    * are fed through this scan while elements on the right are fed through the
    * suppplied scan. The outputs are joined together.
    */
  def choice[S2, I2, O2 >: O](that: EvalScan[F, S2, I2, O2]): EvalScan[F, (S, S2), Either[I, I2], O2] =
    EvalScan[F, (S, S2), Either[I, I2], O2]((initial, that.initial))(
      { case ((s, s2), e) =>
        e match {
          case Left(i) =>
            transform(s, i).map { case (sp, os) =>
              ((sp, s2), os)
            }
          case Right(i2) =>
            that.transform(s2, i2).map { case (s2p, o2s) =>
              ((s, s2p), o2s)
            }
        }
      },
      { case (s, s2) => onComplete(s).flatMap(o => that.onComplete(s2).map(o ++ _)) }
    )

  /** Like [[choice]] but the output elements are kept separate. */
  def choose[S2, I2, O2](that: EvalScan[F, S2, I2, O2]): EvalScan[F, (S, S2), Either[I, I2], Either[O, O2]] =
    EvalScan[F, (S, S2), Either[I, I2], Either[O, O2]]((initial, that.initial))(
      { case ((s, s2), e) =>
        e match {
          case Left(i) =>
            transform(s, i).map { case (sp, os) =>
              ((sp, s2), os.map(Left(_)))
            }
          case Right(i2) =>
            that.transform(s2, i2).map { case (s2p, o2s) =>
              ((s, s2p), o2s.map(Right(_)))
            }
        }
      },
      { case (s, s2) =>
        onComplete(s).flatMap { o =>
          that.onComplete(s2).map { o2 =>
            o.map(Left(_)) ++ o2.map(Right(_))
          }
        }
      }
    )
}

object EvalScan {

  def apply[F[_]: Monad, S, I, O](
      initial: S
  )(transform: (S, I) => F[(S, Chunk[O])], onComplete: S => F[Chunk[O]]): EvalScan[F, S, I, O] =
    new EvalScan(initial, AndThen { case (s, i) => transform(s, i) }, AndThen(onComplete))

  def stateful[F[_]: Monad, S, I, O](initial: S)(transform: (S, I) => F[(S, Chunk[O])]): EvalScan[F, S, I, O] =
    apply(initial)(transform, _ => Chunk.empty[O].pure[F])

  def stateful1[F[_]: Monad, S, I, O](initial: S)(f: (S, I) => F[(S, O)]): EvalScan[F, S, I, O] =
    stateful[F, S, I, O](initial) { (s, i) =>
      f(s, i).map { case (s2, o) =>
        (s2, Chunk.singleton(o))
      }
    }

  def stateless[F[_]: Monad, I, O](f: I => F[Chunk[O]]): EvalScan[F, Unit, I, O] =
    stateful[F, Unit, I, O](())((u, i) => f(i).map((u, _)))

  def lift[F[_]: Monad, I, O](f: I => F[O]): EvalScan[F, Unit, I, O] =
    stateless(i => f(i).map(Chunk.singleton))

  implicit def functor[F[_], S, I]: Functor[EvalScan[F, S, I, *]] =
    new Functor[EvalScan[F, S, I, *]] {
      def map[O, O2](s: EvalScan[F, S, I, O])(f: O => O2): EvalScan[F, S, I, O2] = s.map(f)
    }

  implicit def contravariant[F[_], S, O]: Contravariant[EvalScan[F, S, *, O]] =
    new Contravariant[EvalScan[F, S, *, O]] {
      def contramap[I, I2](s: EvalScan[F, S, I, O])(f: I2 => I): EvalScan[F, S, I2, O] = s.contramap(f)
    }

  implicit def strong[F[_], S]: Strong[EvalScan[F, S, *, *]] = new Strong[EvalScan[F, S, *, *]] {
    def first[A, B, C](fa: EvalScan[F, S, A, B]): EvalScan[F, S, (A, C), (B, C)]  = fa.first
    def second[A, B, C](fa: EvalScan[F, S, A, B]): EvalScan[F, S, (C, A), (C, B)] = fa.second
    def dimap[A, B, C, D](fab: EvalScan[F, S, A, B])(f: C => A)(g: B => D): EvalScan[F, S, C, D] =
      fab.dimap(f)(g)
  }
}
