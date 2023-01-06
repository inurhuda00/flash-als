const ratings = document.querySelectorAll("input")
// ganti localStorage ke cookie

async function addRating(movieId, rating) {
    fetch("/rating/add", {
        method: "POST",
        body: JSON.stringify({ movieId, rating }),
        headers: { "Content-Type": "application/json" },
    })
}

async function updateRating(movieId, rating) {
    fetch("/rating/update", {
        method: "POST",
        body: JSON.stringify({ movieId, rating }),
        headers: { "Content-Type": "application/json" },
    })
}

ratings.forEach((rating) => {
    return rating.addEventListener("click", (e) => {
        e.stopPropagation()

        const { checked, id, value } = e.currentTarget
        const movieId = Number(id.split("-")[0])
        const rating = Number(value)

        if (checked) {
            const duplicate = movies_stars.some(function (item, idx) {
                return Number(item.movieId) == movieId
            })

            !duplicate ? addRating(movieId, rating) : updateRating(movieId, rating)
        }
    })
})
