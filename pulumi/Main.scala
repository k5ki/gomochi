import besom.*
import besom.api.aws
import besom.api.aws.sqs
import besom.api.aws.sqs.QueueArgs

@main def main = Pulumi.run {
  val queue = aws.sqs.Queue(
    "omochi",
    QueueArgs(
      fifoQueue = true,
      messageRetentionSeconds = 60 * 60 * 24
    )
  )

  Stack.exports(
    queueUrl = queue.url
  )
}
