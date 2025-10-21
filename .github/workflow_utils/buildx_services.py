import subprocess
import sys

import yaml


def get_services_to_build(compose_file="docker-compose.yml"):
    """
    Parses a docker-compose file and returns a list of services to build.
    """
    try:
        with open(compose_file, "r") as f:
            doc = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: {compose_file} not found.", file=sys.stderr)
        return []

    services_to_build = []
    services = doc.get("services", {})
    for svc, details in services.items():
        if "build" in details:
            if isinstance(details["build"], str):
                context = details["build"]
                dockerfile = "Dockerfile"
            elif isinstance(details["build"], dict):
                context = details["build"].get("context", ".")
                dockerfile = details["build"].get("dockerfile", "Dockerfile")
            else:
                context = None
                dockerfile = None

            if context:
                services_to_build.append(
                    {
                        "name": svc,
                        "context": context,
                        "dockerfile": dockerfile,
                    }
                )
    return services_to_build


def main():
    """
    Main function to orchestrate the build process with buildx caching.
    """
    services = get_services_to_build()
    if not services:
        print("No services with build context found. Exiting.")
        return

    print("Detected services and building per-service with buildx cache.")
    for svc_info in services:
        svc = svc_info["name"]
        context = svc_info["context"]
        dockerfile = svc_info["dockerfile"]

        print(f"Processing service: {svc}")
        print(f"Building {svc} from context={context} dockerfile={dockerfile}")

        try:
            subprocess.run(
                [
                    "docker",
                    "buildx",
                    "build",
                    "--file",
                    f"{context}/{dockerfile}",
                    "--tag",
                    f"local-build/{svc}:latest",
                    "--load",
                    "--cache-from=type=gha",
                    "--cache-to=type=gha,mode=max",
                    context,
                ],
                check=True,
            )
            print(f"Successfully built {svc}\n")
        except subprocess.CalledProcessError as e:
            print(f"Error building service {svc}: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
