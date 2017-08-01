require 'delayed_job'

module ActiveJob
  module QueueAdapters
    class DelayedJobAdapter

      class << self
        def enqueue(job) #:nodoc:
          delayed_job = Delayed::Job.enqueue(JobWrapper.new(job.serialize), queue: job.queue_name, priority: job.priority)
          job.provider_job_id = delayed_job.id
          delayed_job
        end

        def enqueue_at(job, timestamp) #:nodoc:
          delayed_job = Delayed::Job.enqueue(JobWrapper.new(job.serialize), queue: job.queue_name, priority: job.priority, run_at: Time.at(timestamp))
          job.provider_job_id = delayed_job.id
          delayed_job
        end
      end

      class JobWrapper #:nodoc:
        attr_accessor :job_data

        def initialize(job_data)
          @job_data = job_data
        end

        def perform
          Base.execute(job_data)
        end
      end
    end
  end
end



