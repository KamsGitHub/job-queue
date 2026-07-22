-- CreateEnum
CREATE TYPE "JobPriority" AS ENUM ('LOW', 'NORMAL', 'HIGH');

-- AlterTable
ALTER TABLE "Job" ADD COLUMN "priority" "JobPriority" NOT NULL DEFAULT 'NORMAL';
